import java.io.File

val newVersion = args[0]
//val newVersion = 0.5


fun PatchVersion.fixInFile(file: File) {
    val transformedSetup: String = file.readLines().map {
        val prefix = """    implementation "com.github.holgerbrandl:krangl:"""
        val gradleFix = if (it.startsWith(prefix)) {
            """    implementation "com.github.holgerbrandl:krangl:${newVersion}""""
        } else {
            it
        }

        // todo also fix badge here
        val matchGroup = "Central-([0-9.]+)-orange.*".toRegex().find(gradleFix)
        if(matchGroup!=null){
            gradleFix.replace(matchGroup.groupValues[1], newVersion)
        }else{
            gradleFix
        }
    }.joinToString(System.lineSeparator())


    file.writeText(transformedSetup)
}

fixInFile(File("docs/userguide/docs/index.md"))
fixInFile(File("README.md"))

println("patched docs to new version ${newVersion}")

