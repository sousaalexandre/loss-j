from pathlib import Path
from loguru import logger
import torch
import time
from typing import Any, Optional
from typing_extensions import override
import re

from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions, PictureDescriptionVlmOptions
from docling_core.types.doc import ImageRefMode
from hierarchical.postprocessor import ResultPostprocessor
from src.settings import VLM_MODEL_NAME

# ---- NEW: custom markdown serializer imports ----
from docling_core.transforms.serializer.base import BaseDocSerializer, SerializationResult
from docling_core.transforms.serializer.common import create_ser_result
from docling_core.transforms.serializer.markdown import (
    MarkdownDocSerializer,
    MarkdownParams,
    MarkdownPictureSerializer,
)
from docling_core.types.doc.document import DoclingDocument, PictureItem

try:
    from docling.datamodel.pipeline_options import AcceleratorDevice
except Exception:
    AcceleratorDevice = None


BACKUP_VLM = "HuggingFaceTB/SmolVLM-256M-Instruct"


def _picture_description_stats(doc) -> tuple[int, int]:
    pics = list(getattr(doc, "pictures", []) or [])
    total = len(pics)
    described = 0
    for p in pics:
        try:
            if (
                getattr(p, "meta", None) is not None
                and getattr(p.meta, "description", None) is not None
                and (p.meta.description.text or "").strip()
            ):
                described += 1
        except Exception:
            pass
    return total, described


class PictureDescriptionMarkdownSerializer(MarkdownPictureSerializer):
    def __init__(self, add_placeholder_for_missing=False, drop_images_without_description=True):
        super().__init__()
        self.add_placeholder_for_missing = add_placeholder_for_missing
        self.drop_images_without_description = drop_images_without_description

    @override
    def serialize(self, *, item: PictureItem, doc_serializer: BaseDocSerializer, doc: DoclingDocument,
                  separator: Optional[str] = None, **kwargs: Any) -> SerializationResult:

        parent_res = super().serialize(item=item, doc_serializer=doc_serializer, doc=doc, **kwargs)

        raw = ""
        try:
            if item.meta and item.meta.description:
                raw = (item.meta.description.text or "")
        except Exception:
            raw = ""

        desc = _clean_picture_desc(raw)

        if not desc and self.drop_images_without_description:
            return create_ser_result(text="", span_source=item)

        parts = []
        if parent_res.text:
            parts.append(parent_res.text)

        if desc:
            # Não repetir se já estiver embutido (por alguma versão do Docling)
            if desc not in (parent_res.text or ""):
                parts.append(desc)
        elif self.add_placeholder_for_missing:
            parts.append("Imagem sem descrição gerada (possível elemento decorativo ou recorte inválido).")

        text_res = (separator or "\n\n").join([p for p in parts if p])
        return create_ser_result(text=text_res, span_source=item)


def _build_docling_converter(
    images_scale: float,
    picture_desc_repo: str,
    use_cuda: bool,
) -> DocumentConverter:
    pipeline_options = PdfPipelineOptions()

    pipeline_options.images_scale = images_scale
    pipeline_options.generate_picture_images = True

    # Enable image descriptions
    pipeline_options.do_picture_description = True
    pipeline_options.picture_description_options = PictureDescriptionVlmOptions(
        repo_id=picture_desc_repo,
        batch_size=1,
        scale=1.0,
        prompt=(
        "Tarefa: descrever a imagem para RAG.\n"
        "Regras de formato (obrigatórias):\n"
        "- Escreve APENAS um único parágrafo conciso.\n"
        "- Não uses listas, marcadores, subtítulos, nem cabeçalhos.\n"
        "- Não escrevas prefácios tipo 'Aqui está...' nem rótulos como 'Descrição:' ou 'Elementos principais:'.\n"
        "- Se for gráfico/diagrama/mapa: menciona num parágrafo os eixos/legendas/rótulos e a mensagem principal.\n"
        "- Se for decorativa/sem informação: escreve 'Imagem decorativa sem informação técnica relevante.'\n"
        ),
        generation_config={
            "max_new_tokens": 160,
            "do_sample": False,
        },
    )

    if use_cuda and AcceleratorDevice is not None:
        try:
            pipeline_options.accelerator_options.device = AcceleratorDevice.CUDA
        except Exception as e:
            logger.warning(f"Could not set Docling accelerator to CUDA: {e}")

    return DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
    )

def _clean_picture_desc(text: str) -> str:
    if not text:
        return ""

    t = text.strip()

    # Remove prefácios comuns
    t = re.sub(r"^Aqui está.*?:\s*", "", t, flags=re.IGNORECASE | re.DOTALL)

    # Remove cabeçalhos/rótulos típicos
    t = re.sub(r"(?im)^\s*\*\*(descrição|descrição da imagem|elementos principais|elementos[- ]chave|componentes principais)\*\*\s*:?\s*", "", t)
    t = re.sub(r"(?im)^\s*(descrição|descrição da imagem|elementos principais|elementos[- ]chave|componentes principais)\s*:?\s*", "", t)

    # Remove headings markdown tipo "### ..." ou "**Descrição:**" no meio
    t = re.sub(r"(?m)^\s*#{1,6}\s+.*$", "", t)

    # Converte bullets em frases corridas
    t = re.sub(r"(?m)^\s*[\*\-\u2022]\s+", "", t)

    # Remove negritos/itálicos soltos que só criam ruído (opcional)
    t = t.replace("**", "").replace("*", "")

    # Normaliza whitespace e colapsa em 1 parágrafo
    t = " ".join(t.split())

    # Limpa pontuação duplicada
    t = re.sub(r"\s+([,.;:])", r"\1", t)

    return t.strip()

def _dedupe_image_descriptions_around_placeholder(md: str, image_placeholder: str = "<!-- image -->") -> str:
    """
    Remove duplicação no padrão:
      <descrição>\n\n<!-- image -->\n\n<mesma descrição>
    Mantém apenas:
      <!-- image -->\n\n<descrição>

    Assume que a descrição já foi normalizada para 1 parágrafo (sem quebras de linha internas).
    """
    ph = re.escape(image_placeholder)

    # descrição = linha/parágrafo sem linhas vazias no meio (após _clean_picture_desc tende a ser 1 linha)
    pattern = re.compile(
        rf"(?P<desc>[^\n][^\n]*)\n{{2,}}{ph}\n{{2,}}(?P=desc)(?=\n{{2,}}|\Z)"
    )

    prev = None
    while md != prev:
        prev = md
        md = pattern.sub(lambda m: f"{image_placeholder}\n\n{m.group('desc')}", md)

    return md


def parse_pdf_docling(
    pdf_path: str,
    output_dir: str,
    images_scale: float = 2.0,
    fallback_to_smol_on_failure: bool = True,
    add_placeholder_for_missing_desc: bool = False,
    drop_images_without_desc: bool = True,
) -> tuple[Path, Path]:
    """
    Convert PDF -> Markdown (Docling), export images, and include Docling image descriptions in Markdown.
    Returns: (md_path, artifacts_dir)
    """
    try:
        pdf_path = Path(pdf_path).expanduser().resolve()

        out_dir = Path(output_dir).expanduser().resolve(strict=False)
        out_dir.mkdir(parents=True, exist_ok=True)

        base_name = pdf_path.stem
        md_filename = f"{base_name}.md"
        md_path = out_dir / md_filename

        artifacts_dir = out_dir / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)

        has_cuda = torch.cuda.is_available()
        if has_cuda:
            logger.info("CUDA detected -> using Gemma 3 for image descriptions")
        else:
            logger.warning(
                "CUDA not detected. Gemma 3 image descriptions may fail on CPU "
                "(oneDNN/bfloat16 conv issue) and are very slow."
            )

        converter = _build_docling_converter(
            images_scale=images_scale,
            picture_desc_repo=VLM_MODEL_NAME,
            use_cuda=has_cuda,
        )

        try:
            t0 = time.time()
            logger.info("Starting convert()...")
            result = converter.convert(str(pdf_path))
            logger.info(f"convert() done in {time.time()-t0:.1f}s")

        except RuntimeError as e:
            err = str(e)
            gemma_cpu_conv_error = (
                "primitive descriptor for a convolution forward propagation primitive" in err
                or "Pipeline StandardPdfPipeline failed" in err
            )

            if fallback_to_smol_on_failure and gemma_cpu_conv_error:
                logger.warning(
                    "Gemma-3 failed during picture description (likely CPU oneDNN/bfloat16 "
                    "conv issue). Falling back to SmolVLM-256M-Instruct for this document."
                )
                converter = _build_docling_converter(
                    images_scale=images_scale,
                    picture_desc_repo=BACKUP_VLM,
                    use_cuda=has_cuda,
                )
                result = converter.convert(str(pdf_path))
            else:
                raise

        total_before, described_before = _picture_description_stats(result.document)
        logger.info(
            f"Picture descriptions BEFORE postprocess: {described_before}/{total_before}"
        )

        t1 = time.time()
        logger.info("Starting postprocess()...")
        ResultPostprocessor(result).process()
        logger.info(f"postprocess() done in {time.time()-t1:.1f}s")

        total_after, described_after = _picture_description_stats(result.document)
        logger.info(
            f"Picture descriptions AFTER postprocess: {described_after}/{total_after}"
        )

        # 1) Save once to export artifacts and make image refs available
        t2 = time.time()
        logger.info("Starting save_as_markdown() [artifacts export]...")
        result.document.save_as_markdown(
            md_path,
            artifacts_dir=artifacts_dir,
            image_mode=ImageRefMode.REFERENCED,
        )
        logger.info(f"save_as_markdown() done in {time.time()-t2:.1f}s")

        # 2) Re-serialize markdown so each image gets an immediate description/placeholder
        t3 = time.time()
        logger.info("Starting custom markdown serialization (force image descriptions)...")
        serializer = MarkdownDocSerializer(
            doc=result.document,
            picture_serializer=PictureDescriptionMarkdownSerializer(
                add_placeholder_for_missing=add_placeholder_for_missing_desc,
                drop_images_without_description=drop_images_without_desc,
            ),
            params=MarkdownParams(
                image_mode=ImageRefMode.REFERENCED,
                image_placeholder="<!-- image -->",
            ),
        )
        md_text = serializer.serialize().text
        md_text = _dedupe_image_descriptions_around_placeholder(md_text, "<!-- image -->")
        md_path.write_text(md_text, encoding="utf-8")
        logger.info(f"custom markdown serialization done in {time.time()-t3:.1f}s")

        logger.info(f"-> SUCCESS: Saved MD='{md_filename}' and artifacts='{artifacts_dir}'")
        return md_path, artifacts_dir

    except Exception as e:
        logger.exception(f"Error parsing PDF with images + descriptions: {e}")
        raise