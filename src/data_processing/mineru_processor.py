import asyncio
import copy
import json
import os
import httpx
import requests
from pathlib import Path
from loguru import logger

from mineru.cli.common import convert_pdf_bytes_to_bytes_by_pypdfium2, prepare_env, read_fn
from mineru.data.data_reader_writer import FileBasedDataWriter
from mineru.utils.draw_bbox import draw_layout_bbox, draw_span_bbox
from mineru.utils.enum_class import MakeMode
from mineru.backend.vlm.vlm_analyze import doc_analyze as vlm_doc_analyze
from mineru.backend.pipeline.pipeline_analyze import doc_analyze as pipeline_doc_analyze
from mineru.backend.pipeline.pipeline_middle_json_mkcontent import union_make as pipeline_union_make
from mineru.backend.pipeline.model_json_to_middle_json import result_to_middle_json as pipeline_result_to_middle_json
from mineru.backend.vlm.vlm_middle_json_mkcontent import union_make as vlm_union_make


def _check_vlm_server_health(server_url: str, timeout: int = 10) -> bool:
    """Check if the VLM server is alive."""
    try:
        health_url = f"{server_url.rstrip('/')}/health"
        response = requests.get(health_url, timeout=timeout)
        response.raise_for_status()
        logger.info(f"VLM server health check passed: {health_url}")
        return True
    except Exception as e:
        raise ConnectionError(f"VLM server unreachable: {e}")

def pdf_to_md(
    pdf_path: str,
    output_dir: str = "output",
    lang: str = "pt",
    backend: str = "pipeline",
    method: str = "auto",
    formula_enable: bool = True,
    table_enable: bool = True,
    server_url: str | None = None,
    start_page_id: int = 0,
    end_page_id: int | None = None,
    make_md_mode: MakeMode = MakeMode.MM_MD
) -> str:
    """
    Parse a single PDF file and return the markdown content.
    
    Parameters:
        pdf_path: Path to the PDF file (e.g., "single_file.pdf")
        output_dir: Output directory for temporary files (images)
        lang: Language for OCR - 'ch', 'en', 'korean', 'japan', 'pt', etc. (pipeline only)
        backend: Backend for parsing:
            - "pipeline": General purpose (default)
            - "vlm-transformers": VLM with transformers
            - "vlm-mlx-engine": VLM with MLX (macOS 13.5+)
            - "vlm-vllm-engine": VLM with vLLM
            - "vlm-http-client": VLM via HTTP
        method: Parsing method - "auto", "txt", or "ocr" (pipeline only)
        formula_enable: Enable formula parsing (pipeline only)
        table_enable: Enable table parsing (pipeline only)
        server_url: Server URL when backend="vlm-http-client"
        start_page_id: First page to parse (0-indexed)
        end_page_id: Last page to parse (None = all pages)
        make_md_mode: Markdown generation mode
        
    Returns:
        str: The markdown content extracted from the PDF
    """
    
    vlm_health_checked = False


    try:
        if backend == "vlm-http-client" and not vlm_health_checked:
            if not server_url:
                raise ValueError("server_url is required for vlm-http-client backend")
            try:
                _check_vlm_server_health(server_url)
                vlm_health_checked = True
            except Exception as e:
                logger.warning(f"VLM health check failed: {e}. Falling back to pipeline backend.")
                backend = "pipeline"

        pdf_name = Path(pdf_path).stem
        pdf_bytes = read_fn(pdf_path)

        if backend == "pipeline":
            # Pipeline backend
            pdf_bytes = convert_pdf_bytes_to_bytes_by_pypdfium2(pdf_bytes, start_page_id, end_page_id)

            infer_results, all_image_lists, all_pdf_docs, lang_list, ocr_enabled_list = pipeline_doc_analyze(
                [pdf_bytes], [lang], parse_method=method, formula_enable=formula_enable, table_enable=table_enable
            )

            model_list = infer_results[0]
            images_list = all_image_lists[0]
            pdf_doc = all_pdf_docs[0]
            _lang = lang_list[0]
            _ocr_enable = ocr_enabled_list[0]

            local_image_dir, local_md_dir = prepare_env(output_dir, pdf_name, method)
            image_writer = FileBasedDataWriter(local_image_dir)

            middle_json = pipeline_result_to_middle_json(
                model_list, images_list, pdf_doc, image_writer, _lang, _ocr_enable, formula_enable
            )

            pdf_info = middle_json["pdf_info"]
            image_dir = str(os.path.basename(local_image_dir))

            # Generate and return markdown content
            md_content_str = pipeline_union_make(pdf_info, make_md_mode, image_dir)
            logger.info(f"Pipeline parsing complete for {pdf_path}")
            return md_content_str

        else:
            # VLM backend
            if backend.startswith("vlm-"):
                backend = backend[4:]

            parse_method = "vlm"

            pdf_bytes = convert_pdf_bytes_to_bytes_by_pypdfium2(pdf_bytes, start_page_id, end_page_id)
            local_image_dir, local_md_dir = prepare_env(output_dir, pdf_name, parse_method)
            image_writer = FileBasedDataWriter(local_image_dir)

            middle_json, infer_result = vlm_doc_analyze(
                pdf_bytes, image_writer=image_writer, backend=backend, server_url=server_url
            )

            pdf_info = middle_json["pdf_info"]
            image_dir = str(os.path.basename(local_image_dir))

            # Generate and return markdown content
            md_content_str = vlm_union_make(pdf_info, make_md_mode, image_dir)
            logger.info(f"VLM parsing complete for {pdf_path}")
            return md_content_str
    except (httpx.ReadError, httpx.ConnectError, httpx.TimeoutException, asyncio.TimeoutError) as e:
        logger.warning(f"VLM processing failed: {type(e).__name__}. Falling back to pipeline backend.")
        backend = "pipeline"
        return pdf_to_md(
            pdf_path=pdf_path,
            output_dir=output_dir,
            lang=lang,
            backend=backend,
            method=method,
            formula_enable=formula_enable,
            table_enable=table_enable,
            server_url=server_url,
            start_page_id=start_page_id,
            end_page_id=end_page_id,
            make_md_mode=make_md_mode
        )
    except Exception as e:
        logger.error(f"Error parsing PDF: {e}")
        raise
        



##### NORMAL MINERU PIPELINE STARTS HERE #####    

def parse_single_pdf_default(
    pdf_path: str,
    output_dir: str = "output",
    lang: str = "pt",
    backend: str = "pipeline",
    method: str = "auto",
    formula_enable: bool = True,
    table_enable: bool = True,
    server_url: str | None = None,
    start_page_id: int = 0,
    end_page_id: int | None = None,
    draw_layout_bbox: bool = True,
    draw_span_bbox: bool = True,
    dump_orig_pdf: bool = True,
    dump_md: bool = True,
    dump_content_list: bool = True,
    dump_middle_json: bool = True,
    dump_model_output: bool = True,
    make_md_mode: MakeMode = MakeMode.MM_MD,
):
    """
    Parse a single PDF file with all features from the original main.py.
    
    Parameters:
        pdf_path: Path to the PDF file (e.g., "single_file.pdf")
        output_dir: Output directory for storing parsing results
        lang: Language for OCR - 'ch', 'en', 'korean', 'japan', etc. (pipeline only)
        backend: Backend for parsing:
            - "pipeline": General purpose (default)
            - "vlm-transformers": VLM with transformers
            - "vlm-mlx-engine": VLM with MLX (macOS 13.5+)
            - "vlm-vllm-engine": VLM with vLLM
            - "vlm-http-client": VLM via HTTP
        method: Parsing method - "auto", "txt", or "ocr" (pipeline only)
        formula_enable: Enable formula parsing (pipeline only)
        table_enable: Enable table parsing (pipeline only)
        server_url: Server URL when backend="vlm-http-client"
        start_page_id: First page to parse (0-indexed)
        end_page_id: Last page to parse (None = all pages)
        draw_layout_bbox: Draw layout bounding boxes
        draw_span_bbox: Draw span bounding boxes
        dump_orig_pdf: Save original PDF copy
        dump_md: Generate markdown file
        dump_content_list: Generate content list JSON
        dump_middle_json: Generate middle JSON
        dump_model_output: Generate model output JSON
        make_md_mode: Markdown generation mode
    """
    try:
        pdf_name = Path(pdf_path).stem
        pdf_bytes = read_fn(pdf_path)

        if backend == "pipeline":
            # Pipeline backend
            pdf_bytes = convert_pdf_bytes_to_bytes_by_pypdfium2(pdf_bytes, start_page_id, end_page_id)

            infer_results, all_image_lists, all_pdf_docs, lang_list, ocr_enabled_list = pipeline_doc_analyze(
                [pdf_bytes], [lang], parse_method=method, formula_enable=formula_enable, table_enable=table_enable
            )

            model_list = infer_results[0]
            model_json = copy.deepcopy(model_list)
            images_list = all_image_lists[0]
            pdf_doc = all_pdf_docs[0]
            _lang = lang_list[0]
            _ocr_enable = ocr_enabled_list[0]

            local_image_dir, local_md_dir = prepare_env(output_dir, pdf_name, method)
            image_writer = FileBasedDataWriter(local_image_dir)
            md_writer = FileBasedDataWriter(local_md_dir)

            middle_json = pipeline_result_to_middle_json(
                model_list, images_list, pdf_doc, image_writer, _lang, _ocr_enable, formula_enable
            )

            pdf_info = middle_json["pdf_info"]

            _process_output_default(
                pdf_info, pdf_bytes, pdf_name, local_md_dir, local_image_dir,
                md_writer, draw_layout_bbox, draw_span_bbox, dump_orig_pdf,
                dump_md, dump_content_list, dump_middle_json, dump_model_output,
                make_md_mode, middle_json, model_json, is_pipeline=True
            )

        else:
            # VLM backend
            if backend.startswith("vlm-"):
                backend = backend[4:]

            draw_span_bbox = False
            parse_method = "vlm"

            pdf_bytes = convert_pdf_bytes_to_bytes_by_pypdfium2(pdf_bytes, start_page_id, end_page_id)
            local_image_dir, local_md_dir = prepare_env(output_dir, pdf_name, parse_method)
            image_writer = FileBasedDataWriter(local_image_dir)
            md_writer = FileBasedDataWriter(local_md_dir)

            middle_json, infer_result = vlm_doc_analyze(
                pdf_bytes, image_writer=image_writer, backend=backend, server_url=server_url
            )

            pdf_info = middle_json["pdf_info"]

            _process_output_default(
                pdf_info, pdf_bytes, pdf_name, local_md_dir, local_image_dir,
                md_writer, draw_layout_bbox, draw_span_bbox, dump_orig_pdf,
                dump_md, dump_content_list, dump_middle_json, dump_model_output,
                make_md_mode, middle_json, infer_result, is_pipeline=False
            )

        logger.info(f"Single PDF parsing complete. Output: {local_md_dir}")
        return local_md_dir

    except Exception as e:
        logger.exception(f"Error parsing single PDF: {e}")
        raise


def _process_output_default(
    pdf_info,
    pdf_bytes,
    pdf_file_name,
    local_md_dir,
    local_image_dir,
    md_writer,
    f_draw_layout_bbox,
    f_draw_span_bbox,
    f_dump_orig_pdf,
    f_dump_md,
    f_dump_content_list,
    f_dump_middle_json,
    f_dump_model_output,
    f_make_md_mode,
    middle_json,
    model_output=None,
    is_pipeline=True
):
    """Process and write output files (internal helper)."""
    if f_draw_layout_bbox:
        draw_layout_bbox(pdf_info, pdf_bytes, local_md_dir, f"{pdf_file_name}_layout.pdf")

    if f_draw_span_bbox:
        draw_span_bbox(pdf_info, pdf_bytes, local_md_dir, f"{pdf_file_name}_span.pdf")

    if f_dump_orig_pdf:
        md_writer.write(f"{pdf_file_name}_origin.pdf", pdf_bytes)

    image_dir = str(os.path.basename(local_image_dir))

    if f_dump_md:
        make_func = pipeline_union_make if is_pipeline else vlm_union_make
        md_content_str = make_func(pdf_info, f_make_md_mode, image_dir)
        md_writer.write_string(f"{pdf_file_name}.md", md_content_str)

    if f_dump_content_list:
        make_func = pipeline_union_make if is_pipeline else vlm_union_make
        content_list = make_func(pdf_info, MakeMode.CONTENT_LIST, image_dir)
        md_writer.write_string(
            f"{pdf_file_name}_content_list.json",
            json.dumps(content_list, ensure_ascii=False, indent=4),
        )

    if f_dump_middle_json:
        md_writer.write_string(
            f"{pdf_file_name}_middle.json",
            json.dumps(middle_json, ensure_ascii=False, indent=4),
        )

    if f_dump_model_output:
        md_writer.write_string(
            f"{pdf_file_name}_model.json",
            json.dumps(model_output, ensure_ascii=False, indent=4),
        )

    logger.info(f"Output directory: {local_md_dir}")


if __name__ == '__main__':
    # Configuration
    filename = "test1.pdf"
    output_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")

    # Example 1: Pipeline backend (default, most general)
    #parse_single_pdf_default(filename, output_root, backend="pipeline")

    # Example 2: VLM backends (uncomment to use)
    # parse_single_pdf(filename, output_root, backend="vlm-transformers")
    # parse_single_pdf(filename, output_root, backend="vlm-mlx-engine")  # macOS 13.5+
    # parse_single_pdf(filename, output_root, backend="vlm-vllm-engine")
    #parse_single_pdf_default(filename, output_root, backend="vlm-http-client", server_url="http://192.168.103.9:30000")
    pdf_to_md(filename, output_root, backend="vlm-http-client", server_url="http://192.168.103.9:30000")
