# Documents Screenshotter

## Purpose
The Documents Screenshotter is a custom recipe designed to automate the process of capturing screenshots of documents. This tool is particularly useful for converting hardcopy documents into digital format by taking screenshots of them, which can then be used for archiving or sharing.

## Parameters
- **input_path**: The file path of the document that needs to be converted into screenshots. This should point to an existing document.
- **output_path**: The directory where the screenshots will be saved. Make sure this directory exists and is writable.
- **screenshot_format**: The format of the screenshot images. Common formats include PNG and JPEG.
- **quality**: The quality of the screenshots if using JPEG format. A value between 1 (worst) and 100 (best).
- **delay**: The delay in seconds before taking the screenshot after the document appears on the screen. This allows time for rendering documents if needed.

## Example Usage
```bash
capture_screenshots --input_path='/path/to/document.pdf' --output_path='./screenshots/' --screenshot_format='png' --quality=100 --delay=2
```
