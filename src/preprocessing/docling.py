from pathlib import Path
import time
from loguru import logger
from hierarchical.postprocessor import ResultPostprocessor
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling_core.types.doc import ImageRefMode



def parse_pdf_docling(
    pdf_path: str,
    output_dir: str,
    images_scale: float = 2.0,
) -> tuple[Path, Path]:
    """
    Convert PDF -> Markdown (Docling), export images.
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

        pipeline_options = PdfPipelineOptions()
        pipeline_options.images_scale = images_scale
        pipeline_options.generate_picture_images = True

        t0 = time.time()
        logger.info("Starting convert()...")
        converter = DocumentConverter(format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)})
        result = converter.convert(str(pdf_path))
        logger.info(f"convert() done in {time.time()-t0:.1f}s")


        t1 = time.time()
        logger.info("Starting postprocess()...")
        ResultPostprocessor(result).process()
        logger.info(f"postprocess() done in {time.time()-t1:.1f}s")

        t2 = time.time()
        logger.info("Starting save_as_markdown() [artifacts export]...")
        result.document.save_as_markdown(
            md_path,
            artifacts_dir=artifacts_dir,
            image_mode=ImageRefMode.REFERENCED,
            )
        logger.info(f"save_as_markdown() done in {time.time()-t2:.1f}s")


        logger.info(f"-> SUCCESS: Saved MD='{md_filename}' and artifacts='{artifacts_dir}'")
        return md_path, artifacts_dir

    except Exception as e:
        logger.exception(f"Error parsing PDF with images: {e}")
        raise