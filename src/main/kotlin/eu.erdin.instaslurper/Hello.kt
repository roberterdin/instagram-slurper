package eu.erdin.instaslurper

import org.asynchttpclient.DefaultAsyncHttpClient
import org.asynchttpclient.DefaultAsyncHttpClientConfig
import org.asynchttpclient.Response
import org.asynchttpclient.filter.ThrottleRequestFilter
import org.litote.kmongo.*
import org.litote.kmongo.MongoOperator.exists
import java.io.File
import java.io.FileNotFoundException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit


data class Post(val _id: org.bson.types.ObjectId?, val postId: String, val imgSmall: String, val imgLarge: String, var downloaded: Boolean)

fun main(args: Array<String>) {
    val BATCH_SIZE = args[2].toInt()

    val client = KMongo.createClient() //get com.mongodb.MongoClient new instance
    val database = client.getDatabase("instagram") //normal java driver usage
    val collection = database.getCollection<Post>(args[0])

    val cf = DefaultAsyncHttpClientConfig.Builder()
            .setUserAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/58.0.3029.110 Chrome/58.0.3029.110 Safari/537.36")
            .setConnectTimeout(5000)
            .addRequestFilter(ThrottleRequestFilter(10))
            .build()
    println("Starting to download pictures from instagram with ${cf.maxConnections} max connections")
    val asyncHttpClient = DefaultAsyncHttpClient(cf)

    while (true) {
        val before = System.nanoTime()
        val toDownload: MutableList<CompletableFuture<Response>> = mutableListOf()
        val successfulDownloads: MutableList<String> = mutableListOf()
        val toDelete: MutableList<String> = mutableListOf()
        val downloadTime: MutableList<Long> = mutableListOf()
        val saveTime: MutableList<Long> = mutableListOf()
        val posts = collection.find("{ downloaded: {$exists: false}}").limit(BATCH_SIZE)
        if (posts.count() == 0) {
            println("All downloaded!")
            break
        }
        println("Downloading $BATCH_SIZE...")
        for (post in posts) {
            val beforeRequest = System.nanoTime()
            val f_small = asyncHttpClient.prepareGet(post.imgSmall)
                    .execute()
                    .toCompletableFuture()
                    .exceptionally { th ->
                        println("Oops, something went wrong ${post.imgSmall}: ${th.message}")
                        null
                    }
                    .thenApply( applyFunc@ {
                        if (it == null || !it.hasResponseBody()){
                            println("${post.postId} returned without responsebody")
                            return@applyFunc it
                        }
                        if (it.hasResponseStatus() && (it.statusCode < 200 || it.statusCode > 299)){
                            println("Not 2xx respose (${post.imgSmall}): ${it.statusCode}")
                            if (it.statusCode == 404){
                                // delete
                                toDelete.add(post._id.toString())
                            }
                            return@applyFunc it
                        }
                        if (it.responseBodyAsBytes.size <= 17){
                            println("${post.postId} response too small to be image.")
                            return@applyFunc it
                        }
                        try {
                            downloadTime.add(System.nanoTime() - beforeRequest)
                            val beforeSave = System.nanoTime()
                            File("${args[1]}${post.postId}.jpg").writeBytes(it.responseBodyAsBytes)
                            saveTime.add(System.nanoTime() - beforeSave)
                            successfulDownloads.add(post._id.toString())
                        } catch (e: FileNotFoundException) {
                            println("Error writing ${post.postId}")
                        }
                        it
                    })
            toDownload.add(f_small)
        }
        toDownload.forEach { e -> e.join() }
        println("${TimeUnit.MILLISECONDS.convert(downloadTime.sum()/downloadTime.size, TimeUnit.NANOSECONDS)}ms avg. download time")
        println("${TimeUnit.MILLISECONDS.convert(saveTime.sum()/saveTime.size, TimeUnit.NANOSECONDS)}ms avg. save time")
        val upRes = collection.updateMany("{_id: {\$in: [${successfulDownloads.map { e -> "ObjectId(\"$e\")" }.joinToString(separator = ", ")}]}}", "{${MongoOperator.set}: {downloaded: true}}")
        println("Flagged ${upRes.modifiedCount} as downloaded")
        val delRes = collection.deleteMany("{_id: {\$in: [${toDelete.map { e -> "ObjectId(\"$e\")" }.joinToString(separator = ", ")}]}}")
        println("Deleted ${delRes.deletedCount} unavailable documents")
        val after = System.nanoTime()
        val elapsed = Math.max((after - before).toDouble()/1000000000, 1.toDouble())
        println("... done! ${successfulDownloads.size}/$BATCH_SIZE downloaded. ${successfulDownloads.size.toDouble()/elapsed} images/s")
    }
}

