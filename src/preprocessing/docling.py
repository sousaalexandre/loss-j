from pathlib import Path
from loguru import logger
from src import settings

from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions, PictureDescriptionVlmOptions
from docling_core.types.doc import ImageRefMode
from hierarchical.postprocessor import ResultPostprocessor

# serializers (docling-core)
from typing import Any, Optional
from typing_extensions import override
from docling_core.transforms.serializer.base import BaseDocSerializer, SerializationResult
from docling_core.transforms.serializer.common import create_ser_result
from docling_core.transforms.serializer.markdown import MarkdownDocSerializer, MarkdownParams, MarkdownPictureSerializer
from docling_core.types.doc.document import DoclingDocument, PictureItem


class PictureDescriptionMarkdownSerializer(MarkdownPictureSerializer):
    """
    Extende o serializer de imagens para anexar a descrição gerada pelo Docling
    (item.meta.description.text) no Markdown.
    """
    @override
    def serialize(
        self,
        *,
        item: PictureItem,
        doc_serializer: BaseDocSerializer,
        doc: DoclingDocument,
        separator: Optional[str] = None,
        **kwargs: Any,
    ) -> SerializationResult:
        parts: list[str] = []

        # Mantém o comportamento padrão (caption + link da imagem, etc.)
        parent_res = super().serialize(item=item, doc_serializer=doc_serializer, doc=doc, **kwargs)
        if parent_res.text:
            parts.append(parent_res.text)

        # Injeta a descrição (normalizada) como texto "visível" no Markdown
        desc = None
        if item.meta is not None and item.meta.description is not None:
            desc = " ".join((item.meta.description.text or "").split())

        if desc:
            parts.append(f"**Descrição da imagem:** {desc}")

        text_res = (separator or "\n\n").join(parts)
        return create_ser_result(text=text_res, span_source=item)


def parse_pdf_docling(
    pdf_path: str,
    output_dir: str,
    images_scale: float = 2.0,
    enable_image_descriptions: bool = True,
    picture_desc_repo_id: str = settings.VLM_MODEL_NAME,
    picture_desc_prompt: str = "Describe this picture in three to five sentences. Be precise and concise.",
) -> tuple[Path, Path]:
    """
    Convert PDF -> Markdown (Docling), exporta imagens e injeta descrições de imagens no Markdown.
    Returns: (md_path, artifacts_dir)
    """
    try:
        pdf_path = Path(pdf_path).expanduser().resolve()

        out_dir = Path(output_dir).expanduser().resolve(strict=False)
        out_dir.mkdir(parents=True, exist_ok=True)

        base_name = pdf_path.stem
        md_path = out_dir / f"{base_name}.md"

        artifacts_dir = out_dir / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)

        pipeline_options = PdfPipelineOptions()
        pipeline_options.images_scale = images_scale
        pipeline_options.generate_picture_images = True

        # 1) Ligar descrições de imagens (VLM) no Docling
        if enable_image_descriptions:
            pipeline_options.do_picture_description = True
            pipeline_options.picture_description_options = PictureDescriptionVlmOptions(
                repo_id=picture_desc_repo_id,
                prompt=picture_desc_prompt,
            )

        converter = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
        )

        result = converter.convert(str(pdf_path))
        ResultPostprocessor(result).process()

        # 2) Exporta imagens (referenciadas) para artifacts e ajusta URIs no doc
        #    (mantém o teu comportamento atual)
        result.document.save_as_markdown(
            md_path,
            artifacts_dir=artifacts_dir,
            image_mode=ImageRefMode.REFERENCED,
        )

        # 3) Re-serializa o Markdown com um picture serializer que inclui a descrição
        serializer = MarkdownDocSerializer(
            doc=result.document,
            picture_serializer=PictureDescriptionMarkdownSerializer(),
            params=MarkdownParams(
                image_mode=ImageRefMode.REFERENCED,  # usa as URIs já criadas no passo anterior
                image_placeholder="<!-- image -->",
            ),
        )
        md_text = serializer.serialize().text
        md_path.write_text(md_text, encoding="utf-8")

        logger.info(f"-> SUCCESS: Saved MD='{md_path.name}' with image descriptions and artifacts='{artifacts_dir}'")
        return md_path, artifacts_dir

    except Exception as e:
        logger.exception(f"Error parsing PDF with images + descriptions: {e}")
        raise