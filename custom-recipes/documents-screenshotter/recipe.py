# recipe.py
import os
import re
import fnmatch
import dataiku
from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config
from dataikuapi.dss.document_extractor import DocumentExtractor, ManagedFolderDocumentRef

# --- Inputs/outputs
client = dataiku.api_client()
project = client.get_project(dataiku.default_project_key())

# Get input and output folders from recipe roles
in_folder_name = get_input_names_for_role("in_docs")[0]
out_folder_name = get_output_names_for_role("out_images")[0]

in_folder = dataiku.Folder(in_folder_name)
out_folder = dataiku.Folder(out_folder_name)

in_mfid = in_folder.get_id()
out_mfid = out_folder.get_id()

# --- Params
cfg = get_recipe_config()

# Path glob filter - disabled in UI but kept in code for potential re-enablement
glob_pat = (cfg.get("path_glob") or "").strip()

start_page = int(cfg.get("start_page", 0))
print(f"Start page setting: {start_page} (0 = start from beginning)")

# OCR - disabled in UI but kept in code for potential re-enablement
run_ocr = bool(cfg.get("run_ocr", False))

# Process mode: "new_only" (default) or "all"
process_mode = cfg.get("process_mode", "new_only")
print(f"Process mode: {process_mode}")

# --- Incremental processing: Track processed documents
def get_processed_documents():
    """Scan output folder to find previously processed documents by basename and track first page number."""
    processed = {}
    try:
        output_paths = out_folder.list_paths_in_partition()
        for path in output_paths:
            # Extract document basename from path structure: {doc_name}/{hash}/page_X.png
            # or {doc_name}/page_X.png
            parts = path.strip('/').split('/')
            if len(parts) > 0:
                doc_basename = parts[0]
                # Initialize document entry if not seen before
                if doc_basename not in processed:
                    try:
                        # Try to get the folder's modification time
                        folder_details = out_folder.get_path_details(doc_basename)
                        processed[doc_basename] = {
                            'basename': doc_basename,
                            'mtime': folder_details.get('lastModified') if folder_details else None,
                            'first_page': None  # Will be set when we find page files
                        }
                    except:
                        processed[doc_basename] = {
                            'basename': doc_basename,
                            'mtime': None,
                            'first_page': None
                        }
                
                # Extract page number from filename (e.g., page_5.png, page5.png, page 5.png -> 5)
                # Look for page_X pattern in the filename (with optional separator)
                filename = parts[-1]  # Last part is the filename
                page_match = re.search(r'page[_\s]?(\d+)', filename, re.IGNORECASE)
                if page_match:
                    page_num = int(page_match.group(1))
                    # Track the minimum page number (first screenshotted page)
                    if processed[doc_basename]['first_page'] is None or page_num < processed[doc_basename]['first_page']:
                        processed[doc_basename]['first_page'] = page_num
    except Exception as e:
        print(f"Warning: Could not scan output folder for processed documents: {e}")
    return processed

def get_document_metadata(folder, path):
    """Get modification time for a document."""
    try:
        # Try to get file details
        details = folder.get_path_details(path)
        if details and 'lastModified' in details:
            return details['lastModified']
        # Fallback: use current time if metadata not available
        return None
    except Exception as e:
        print(f"Warning: Could not get metadata for {path}: {e}")
        return None

# --- List candidate paths
all_paths = in_folder.list_paths_in_partition()

# Optional glob filter
if glob_pat:
    candidates = [p for p in all_paths if fnmatch.fnmatch(p, glob_pat)]
else:
    candidates = list(all_paths)

if not candidates:
    print("No matching documents found, nothing to do.")
    raise SystemExit(0)

# --- Build input documents map with metadata
print("Scanning for incremental processing...")
processed_docs = get_processed_documents()
print(f"Found {len(processed_docs)} previously processed document(s)")

input_docs = {}
for doc_path in candidates:
    # Use full filename (with extension) to match output folder structure
    doc_filename = os.path.basename(doc_path)
    mtime = get_document_metadata(in_folder, doc_path)
    input_docs[doc_filename] = {
        'path': doc_path,
        'mtime': mtime,
        'filename': doc_filename
    }

# --- Categorize documents
new_docs = []
unchanged_docs = []
unchanged_needs_reprocess = []  # Unchanged docs that need reprocessing due to start_page mismatch
deleted_docs = []

for filename, doc_info in input_docs.items():
    if filename not in processed_docs:
        new_docs.append(doc_info)
    else:
        # Check if start_page setting matches the first screenshotted page
        processed_info = processed_docs[filename]
        first_screenshotted_page = processed_info.get('first_page')
        
        # Determine expected first page: start_page if > 0, otherwise 1 (page 1)
        expected_first_page = start_page if start_page > 0 else 1
        
        # If we can't determine the first page or it doesn't match, reprocess
        if first_screenshotted_page is None or first_screenshotted_page != expected_first_page:
            unchanged_needs_reprocess.append(doc_info)
        else:
            unchanged_docs.append(doc_info)

for filename in processed_docs:
    if filename not in input_docs:
        deleted_docs.append(filename)

print(f"Incremental processing summary:")
print(f"  New documents: {len(new_docs)}")
print(f"  Unchanged documents: {len(unchanged_docs)}")
if unchanged_needs_reprocess:
    print(f"  Unchanged documents needing reprocessing (start_page mismatch): {len(unchanged_needs_reprocess)}")
print(f"  Deleted documents: {len(deleted_docs)}")

# Log unchanged documents (only when not processing all)
if unchanged_docs and process_mode == "new_only":
    print(f"\nSkipping {len(unchanged_docs)} unchanged document(s):")
    for doc_info in unchanged_docs:
        print(f"  - {doc_info['path']}")

# Log unchanged documents that need reprocessing due to start_page mismatch
if unchanged_needs_reprocess:
    print(f"\nReprocessing {len(unchanged_needs_reprocess)} document(s) due to start_page mismatch:")
    for doc_info in unchanged_needs_reprocess:
        processed_info = processed_docs[doc_info['filename']]
        first_page = processed_info.get('first_page', 'unknown')
        expected_first_page = start_page if start_page > 0 else 1
        print(f"  - {doc_info['path']} (first page was {first_page}, expected {expected_first_page})")

# --- Cleanup deleted documents
for filename in deleted_docs:
    try:
        print(f"Removing images for deleted document: {filename}")
        out_folder.delete_path(filename)
    except Exception as e:
        print(f"Warning: Could not remove images for {filename}: {e}")

# --- Cleanup documents that need reprocessing due to start_page mismatch
for doc_info in unchanged_needs_reprocess:
    try:
        filename = doc_info['filename']
        print(f"Removing old screenshots for '{filename}' (start_page mismatch)")
        out_folder.delete_path(filename)
    except Exception as e:
        print(f"Warning: Could not remove old screenshots for {doc_info['path']}: {e}")

# --- Prepare documents to process
if process_mode == "all":
    # Process all documents (new + unchanged + unchanged_needs_reprocess)
    docs_to_process = new_docs + unchanged_docs + unchanged_needs_reprocess
else:
    # Process new documents + unchanged documents with start_page mismatch
    docs_to_process = new_docs + unchanged_needs_reprocess

if not docs_to_process:
    if process_mode == "all":
        print("No documents to process.")
    else:
        print("No documents to process (all unchanged or deleted).")
    raise SystemExit(0)

print(f"Processing {len(docs_to_process)} document(s)...")

# --- Document extractor
doc_ex = DocumentExtractor(client, project.project_key)

# --- Iterate docs
for doc_info in docs_to_process:
    doc_path = doc_info['path']
    filename = doc_info['filename']
    
    if filename in processed_docs:
        print(f"Reprocessing document '{doc_path}'")
    else:
        print(f"Processing new document '{doc_path}'")
    # Build the iterator of page images; let DSS stream directly into output folder
    ref = ManagedFolderDocumentRef(doc_path, in_mfid)

    # NOTE: API supports: output_managed_folder, offset, fetch_size, keep_fetched
    # offset is 0-based (screenshot index), start_page is 1-based (page number)
    kwargs = {
        "output_managed_folder": out_mfid
    }
    
    # Map start_page (1-based) to offset (0-based)
    if start_page > 0:
        offset_value = start_page - 1
        kwargs["offset"] = offset_value
        print(f"  Using offset={offset_value} (starting from page {start_page})")
    else:
        print(f"  No offset (processing all pages from start)")
    
    # OCR parameter - may not be supported in all DSS versions
    if run_ocr:
        kwargs["ocr"] = run_ocr
    
    page_iter = doc_ex.generate_pages_screenshots(ref, **kwargs)

    # Consume the iterator - files are saved by the API with default naming
    page_count = 0
    for image_ref in page_iter:
        page_count += 1
    
    if start_page > 0:
        print(f"  Processed {page_count} page(s) starting from page {start_page}")
    else:
        print(f"  Processed {page_count} page(s) total")

print("Done: page screenshots generated.")