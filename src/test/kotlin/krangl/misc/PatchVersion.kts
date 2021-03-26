import java.io.File

val newVersion = args[0]
//val newVersion = 0.5


fun PatchVersion.fixInFile(file: File) {
    val transformedSetup: String = file.readLines().map {
        val prefix: String = """    implementation "com.github.holgerbrandl:krangl:"""
        if (it.startsWith(prefix)) {
            """    implementation "com.github.holgerbrandl:krangl:${newVersion}""""
        } else {
            it
        }

        // todo also fix badge here
//        if(it.contains("Central-[0-9.]*-orange".toRegex())){
//          it
//        }else{
            it
//        }
    }.joinToString(System.lineSeparator())


    file.writeText(transformedSetup)
}

fixInFile(File("docs/userguide/docs/index.md"))
fixInFile(File("README.md"))

println("patched docs to new version ${newVersion}")

