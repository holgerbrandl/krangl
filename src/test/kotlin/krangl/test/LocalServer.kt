package krangl.test

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import java.io.File
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import kotlin.text.Charsets.UTF_8

/**
 * A simple local HTTP file server. Note that the [port] is dynamically assigned.
 */
class LocalServer(vararg handlers: Pair<String, File>) : AutoCloseable {

    private val server: HttpServer

    val port get() = server.address.port

    init {
        server = HttpServer.create(InetSocketAddress("localhost", 0), 0).apply {
            executor = Executors.newFixedThreadPool(1)
            handlers.forEach { this.createContext(it.first, FileHandler(it.second)) }
            start()
        }
    }

    override fun close() = server.stop(0)
}

/**
 * Handles the [file] by writing out it's content
 */
class FileHandler(val file: File) : HttpHandler {
    override fun handle(exchange: HttpExchange) {
        exchange.responseBody.use { os ->
            val content = file.readText(UTF_8)
            exchange.sendResponseHeaders(200, content.length.toLong())
            os.write(content.toByteArray(UTF_8))
            os.flush()
            println("### served: ${file.absolutePath}")
        }
    }
}
