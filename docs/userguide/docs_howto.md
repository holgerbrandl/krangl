# Documentation Build

Kalasim documentation is build with [mkdocs](https://www.mkdocs.org/).

```bash
#pip install mkdocs-material
cd   /c/Users/brandl/Dropbox/sharedDB/db_projects/krangl/userguide

#pip install markdown-include
#pip install pymdown-extensions # not needed  

#mkdocs new .

mkdocs serve

mkdocs build
```

For more details see <https://squidfunk.github.io/mkdocs-material/creating-your-site/>


## Tech Pointers

For publishing options see <https://squidfunk.github.io/mkdocs-material/publishing-your-site/>

Nice options overview <https://github.com/squidfunk/mkdocs-material/blob/master/mkdocs.yml>

include code into mkdocs  <https://github.com/mkdocs/mkdocs/issues/777> <https://github.com/cmacmackin/markdown-include>

header stripping ? Not yet, see <https://github.com/cmacmackin/markdown-include/issues/9>

**{todo}** consider using snippets <https://squidfunk.github.io/mkdocs-material/reference/code-blocks/#snippets>


## Charts with Mermaid

mermaid is 10x more popular than plantuml on github

* comparison <https://ruleoftech.com/2018/generating-documentation-as-code-with-mermaid-and-plantuml>
* <https://www.npmtrends.com/mermaid-vs-plantuml-encoder-vs-jointjs-vs-mxgraph>

Nice examples --> <https://github.com/mermaid-js/mermaid>

Online Editor <https://mermaid-js.github.io/mermaid-live-editor>