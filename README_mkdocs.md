MkDocs itself runs locally — it's a Python tool you install on your machine. It reads your markdown files and generates static HTML.
bashpip install mkdocs mkdocs-material
mkdocs serve    # Local dev server at localhost:8000
mkdocs build    # Outputs static HTML to site/ folder
The generated site can be hosted on GitHub Pages for free. Two ways to get it there:
Option 1: Manual Deploy
bashmkdocs gh-deploy
This builds the HTML locally and pushes it to a gh-pages branch in your repo. GitHub Pages serves it automatically at https://glenn-edgar.github.io/repo-name/.
for knowledge_base_contauber https://glenn-edgar.github.io/knowledge_base_container/c-s-engine/README_top_design/
