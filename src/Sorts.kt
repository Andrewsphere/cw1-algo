import java.util.*
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ThreadLocalRandom
import kotlin.system.measureTimeMillis

fun IntArray.swap(a: Int, b: Int) {
    val t = this[a]
    this[a] = this[b]
    this[b] = t
}

fun sortSeq(ar: IntArray, l: Int = 0, r: Int = ar.size - 1) {
    if (l >= r) return

    var start = l
    var end = r
    val pivot = ar[ThreadLocalRandom.current().nextInt(l, r)]

    while (start <= end) {
        while (ar[start] < pivot) start++
        while (ar[end] > pivot) end--
        if (start <= end) {
            ar.swap(start++, end--)
        }
    }

    sortSeq(ar, l, end)
    sortSeq(ar, start, r)
}

const val BLOCK_SIZE = 1 shl 10


private fun partition(ar: IntArray, l: Int, r: Int): Int {
    val pivot = ar[ThreadLocalRandom.current().nextInt(l, r)]
    var start = l - 1
    var end = r + 1
    while (true) {
        do {
            start++
        }
        while (ar[start] < pivot)
        do {
            end --
        }
        while (ar[end] > pivot)
        if (start >= end) return end
        ar.swap(start, end)
    }
}


fun sortPar(ar: IntArray, pool: ForkJoinPool = ForkJoinPool.commonPool(), l: Int = 0, r: Int = ar.size - 1) {
    if (r - l < BLOCK_SIZE) return sortSeq(ar, l, r)

    val m = partition(ar, l, r)

    val f1 = pool.submit {
        sortPar(ar, pool, l, m)
    }
    val f2 = pool.submit {
        sortPar(ar, pool, m + 1, r)
    }
    f1.join()
    f2.join()
}

fun test(size: Int = 50000, count: Int = 1000, maxInt: Int = 2000) {
    for (i in 0 until count) {
        val seq = (0..size).map { ThreadLocalRandom.current().nextInt(maxInt) }.toIntArray()
        val par = seq.clone()
        val check = seq.clone()
        sortSeq(seq)
        sortPar(par)
        Arrays.sort(check)
        if (!seq.contentEquals(check)) {
            println("Test failed for Sequential")
        }
        if (!par.contentEquals(check)) {
            println("Test failed for parallel")
        }
    }
}

fun run(log: Boolean = false): Pair<Long, Long> {
    val ar = (0..100000000).map { ThreadLocalRandom.current().nextInt(1000000) }.toIntArray()
    val ar2 = ar.clone()
    val t1 = measureTimeMillis {
        sortSeq(ar)
    }.also { if (log) println("$it ms seq") }

    val t2 = measureTimeMillis {
        sortPar(ar2, ForkJoinPool(4))
    }.also { if (log) println("$it ms par") }
    return t1 to t2
}

fun main() {

    println("Testing...")
    test()
    println("Testing finished, execution warm-up")
    for (i in 0 until 5) run()
    println("Measuring")
    val result = (0 until 5). map {  run(true)}.fold(0L to 0L) {l, r ->
        l.first + r.first to l.second + r.second
    }

    println("Out 5 average ms for sequential: ${result.first / 5}, for parallel (4 cores): ${result.second / 5}")
    println("Speed-up ratio: ${result.first.toDouble().div(result.second.toDouble())}")
}