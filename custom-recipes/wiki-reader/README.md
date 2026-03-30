# Wiki Reader Custom Recipe

## Purpose
The Wiki Reader custom recipe is designed to facilitate the extraction and parsing of information from various wiki-style knowledge bases, enabling users to easily access, analyze, and utilize content for different applications.

## Parameters
- `source`: This parameter identifies the source of the wiki content. It could be a URL or a database connection string.
- `format`: Defines the format of the retrieved data, such as JSON, XML, or plain text.
- `language`: Specifies the language in which to extract the content, allowing users to get localized information.
- `timeout`: Sets the duration (in seconds) before the request times out, providing a way to manage slower responses effectively.

## Usage
To utilize the Wiki Reader custom recipe, configure the parameters according to your needs and invoke the extraction process. It will handle the data retrieval and parsing, providing you with structured information for further use.

## Examples
1. **Extracting content from Wikipedia**:
   - `source`: `https://en.wikipedia.org/wiki/Special:Random`
   - `format`: `JSON`
   - `language`: `en`
   - `timeout`: `10`

2. **Retrieving data from a corporate wiki**:
   - `source`: `company_wiki_db_connection`
   - `format`: `plaintext`
   - `language`: `es`
   - `timeout`: `5`

---