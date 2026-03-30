> **AE Toolbox** - Agent tools and flow recipes for AE Workflows in Dataiku

A comprehensive toolkit of agent tools, custom recipes, and Python utilities designed for intelligent automation and workflow management.

**Author:** Ali Hashim  
**License:** Apache Software License  
**Version:** 1.0.0

---

## 📋 Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Components](#components)
- [Getting Started](#getting-started)
- [Contributing](#contributing)

---

## Overview

The ae_toolbox is a collection of pre-built agent tools, custom recipes, and reusable Python libraries that enable rapid development of intelligent automation workflows. It provides ready-to-use components for common automation tasks and patterns.

---

## Project Structure

```
ae_toolbox/
├── code-env/                    # Code environment configurations
├── custom-recipes/              # Pre-built workflow recipes
├── python-agent-tools/          # Python-based agent tools
├── python-lib/                  # Shared Python utilities library
└── plugin.json                  # Plugin configuration
```

---

## Components

### 🔧 Custom Recipes

Pre-built workflow recipes for common automation scenarios:

- **[documents-screenshotter](./custom-recipes/documents-screenshotter/)** - Automated document screenshot capture and processing
- **[jira-wiki-agenda](./custom-recipes/jira-wiki-agenda/)** - JIRA and Wiki agenda integration tool
- **[wiki-reader](./custom-recipes/wiki-reader/)** - Wiki content reader and parser

### 🤖 Python Agent Tools

Agent-ready tools built with Python for intelligent automation:

- **[gsg-snowflake-search](./python-agent-tools/gsg-snowflake-search/)** - Snowflake database search integration
- **[snowflake_cortex_search](./python-agent-tools/snowflake_cortex_search/)** - Snowflake Cortex semantic search capabilities

### 📚 Python Library

Shared utilities and core functionality:

- **[gsgtoolbox](./python-lib/gsgtoolbox/)** - Core Python library with reusable components

### 🏗️ Code Environment

Environment-specific configurations:

- **[python](./code-env/python/)** - Python runtime and dependency configurations

---

## Getting Started

### Prerequisites

- Python 3.x
- Access to relevant integrations (JIRA, Wiki, Snowflake, etc.)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/hashali-solid/ae_toolbox.git
   cd ae_toolbox
   ```

2. Install dependencies from the python-lib:
   ```bash
   cd python-lib/gsgtoolbox
   pip install -e .
   ```

3. Configure your environment settings in `code-env/python/`

### Usage

Explore individual component directories for specific usage documentation:

- [Custom Recipes Documentation](./custom-recipes/)
- [Python Agent Tools Documentation](./python-agent-tools/)
- [Python Library Documentation](./python-lib/gsgtoolbox/)

---

## Contributing

Contributions are welcome! Please ensure:

1. Code follows project conventions
2. Tests are included for new functionality
3. Documentation is updated accordingly
4. License headers are included in new files

---

## License

This project is licensed under the **Apache Software License**. See LICENSE file for details.

---

## Plugin Configuration

This toolbox is configured as a plugin with the following settings:

- **Plugin ID:** `gsg-toolbox`
- **Version:** `1.0.0`
- **Description:** Agent tools and flow recipes for AEs
- **Icon:** `fab fa-staylinked`

For more details, see [plugin.json](./plugin.json).

---

For more information or support, please visit the [repository](https://github.com/hashali-solid/ae_toolbox) or contact the maintainer.
