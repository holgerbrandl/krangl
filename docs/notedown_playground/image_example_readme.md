# Hello World

```kotlin
import java.io.File


// some comment

val a = 3
println("foo 2")

```
And another one

```kotlin
//println(File(.).absolutePath)
println("bar")
```

some more text below

and now the grand finale

```kotlin
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import javax.imageio.ImageIO
import java.util.Base64
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.IOException;

val pathToFile = File("/Users/brandl/Downloads/Clipboard.png")

val imageF = ImageIO.read(pathToFile)

// Draw the image on to the buffered image
val bimage = BufferedImage(imageF.getWidth(null), imageF.getHeight(null), BufferedImage.TYPE_INT_RGB)
val bGr = bimage.createGraphics()
bGr.drawImage(imageF, 0, 0, null)
bGr.dispose()

val buf = ByteArrayOutputStream()

val writer = ImageIO.getImageWritersByMIMEType("image/jpeg").next()

val ios = ImageIO.createImageOutputStream(buf)
writer.output = ios
writer.write(bimage)
ios.close()
val b64img = Base64.getEncoder().encodeToString(buf.toByteArray())        
resultOf("image/jpeg" to b64img)
```


some more footer text