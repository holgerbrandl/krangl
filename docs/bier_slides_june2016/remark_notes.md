http://tech.graze.com/2015/07/31/easily-create-slideshow-presentations-from-markdown-with-remark-js/

```
# cd /Users/brandl/Dropbox/Public/jl_presentation
# markdown-to-slides kplyr_intro.md -o kplyr_intro.html
#
# ## or with custom style
# markdown-to-slides -s remark_style.css kplyr_intro.md -o kplyr_intro.html
#
# ## and using document mode
# markdown-to-slides -d -s remark_style.css kplyr_intro.md -o kplyr_intro.html

cd /Users/brandl/projects/kotlin/kplyr/docs/bier_slides_june2016
while :
do
    markdown-to-slides -s remark_style.css kplyr_intro.md -o kplyr_intro.html
    sleep 2
done
```

## Awesome examples

https://raw.githubusercontent.com/greatghoul/remarks/master/markdown/Remarks.md


https://github.com/gnab/remark
http://remarkjs.com/#1

https://gnab.github.io/remark/remarkise
https://gnab.github.io/remark/remarkise?url=https%3A%2F%2Fdl.dropboxusercontent.com%2Fu%2F113630701%2Fjl_presentation%2Fkplyr_intro.md#1


* Code chunkds by language (From http://remarkjs.com/#15)
```javascript
function add(a, b)
  return a + b
end
```

* Pressing P will toggle presenter mode.
* Slide comments after ???
* Pressing C will open a cloned view of the current slideshow in a new browser window.

## From https://github.com/gnab/remark/wiki/Markdown#slide-properties

* Incremental content with --




echo "
<!DOCTYPE html>
<html>
  <head>
    <title>Title</title>
    <meta charset="utf-8">
    <style>
      @import url(https://fonts.googleapis.com/css?family=Yanone+Kaffeesatz);
      @import url(https://fonts.googleapis.com/css?family=Droid+Serif:400,700,400italic);
      @import url(https://fonts.googleapis.com/css?family=Ubuntu+Mono:400,700,400italic);

      body { font-family: 'Droid Serif'; }
      h1, h2, h3 {
        font-family: 'Yanone Kaffeesatz';
        font-weight: normal;
      }
      .remark-code, .remark-inline-code { font-family: 'Ubuntu Mono'; }
    </style>
  </head>
  <body>
    <textarea id="source">

class: center, middle

# Title

---

# Agenda

1. Introduction
2. Deep-dive
3. ...

---

# Introduction

    </textarea>
    <script src="https://gnab.github.io/remark/downloads/remark-latest.min.js">
    </script>
    <script>
      var slideshow = remark.create();
    </script>
  </body>
</html>
" ?


## Updates static html on pages branch

see https://help.github.com/articles/creating-project-pages-manually/

```
cd ~/Desktop
git clone https://github.com/holgerbrandl/kplyr.git
cd kplyr
git checkout gh-pages
## just needed for initial run:
#git rm -rf .
#cp -r /Users/brandl/Dropbox/cluster_sync/kplyr/docs/kplyr_intro .
#rm kplyr_intro/*md kplyr_intro/*.css kplyr_intro/remark_notes.md
# git add -A kplyr_intro

cp /Users/brandl/Dropbox/cluster_sync/kplyr/docs/kplyr_intro/kplyr_intro.html kplyr_intro/
git commit -m "fixed some typos"
git push origin gh-pages

```

http://holgerbrandl.github.io/kplyr/kplyr_intro/kplyr_intro.html