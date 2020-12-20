

## How to improve docs?

* learn from https://jtablesaw.wordpress.com/an-introduction/
* how to illustrate joins https://blog.jooq.org/2016/07/05/say-no-to-venn-diagrams-when-explaining-joins/

* for 10 min overview see http://nbviewer.jupyter.org/urls/gist.github.com/wesm/4757075/raw/a72d3450ad4924d0e74fb57c9f62d1d895ea4574/PandasTour.ipynb
* see https://jtablesaw.github.io/tablesaw/userguide/toc

* add star button directly to docs as in https://deeplearning4j.org/index.html

* Add chapter about integration with https://github.com/bedatadriven/renjin

 * https://blog.jetbrains.com/kotlin/2018/04/embedding-kotlin-playground/

## gitbook documentation

for gitbook page layout see https://toolchain.gitbook.com/pages.html

https://github.com/GitbookIO/gitbook/blob/master/docs/setup.md

```bash
#npm install gitbook-cli -g

#cd /Users/brandl/projects/kotlin/krangl/docs/manual
gitbook serve
gitbook build

gitbook build ./ --log=debug --debug
```

don't miss the local editor
https://www.gitbook.com/editor

https://github.com/GitbookIO/theme-api

nice extensions
https://github.com/GitbookIO/theme-api
https://github.com/jadu/gitbook-theme


for theming add
```
"styles": {
  "website": "assets/continuum/cp.css"
}
```

docs in sub-directory https://github.com/GitbookIO/gitbook/issues/688
