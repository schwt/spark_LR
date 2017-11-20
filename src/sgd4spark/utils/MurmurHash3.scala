package sgd4spark.utils

object MurmurHash3 {

  def murmurhash3_x86_32(data: Array[Byte], offset: Int, len: Int, seed: Int): Int = {
    val c1 = -862048943
    val c2 = 461845907
    var h1 = seed
    val roundedEnd = offset + (len & 0xFFFFFFFC)

    for (i <- offset.until(roundedEnd, 4)) {
      var k1 = data(i) & 0xFF | (data(i + 1) & 0xFF) << 8 | (data(i + 2) & 0xFF) << 16 | data(i + 3) << 24
      k1 *= -862048943
      k1 = k1 << 15 | k1 >>> 17
      k1 *= 461845907
      h1 ^= k1
      h1 = h1 << 13 | h1 >>> 19
      h1 = h1 * 5 + -430675100
    }
    var k1 = 0
    len & 0x3 match {
      case 3 =>
        k1 = (data(roundedEnd + 2) & 0xFF) << 16
      case 2 =>
        k1 |= (data(roundedEnd + 1) & 0xFF) << 8
      case 1 =>
        k1 |= data(roundedEnd) & 0xFF
        k1 *= -862048943
        k1 = k1 << 15 | k1 >>> 17
        k1 *= 461845907
        h1 ^= k1
    }
    h1 ^= len
    h1 ^= h1 >>> 16
    h1 *= -2048144789
    h1 ^= h1 >>> 13
    h1 *= -1028477387
    h1 ^= h1 >>> 16
    h1
  }

  private def unsignedByte(b: Byte): Int = {
    b & 0xFF
  }

  private def getBlock32(data: Array[Byte], i: Int): Int = {
    unsignedByte(data(i)) | unsignedByte(data(i + 1)) << 8 | unsignedByte(data(i + 2)) << 16 | unsignedByte(data(i + 3)) << 24
  }

  private def getBlock64(data: Array[Byte], i: Int): Long = {
    val low = getBlock32(data, 8 * i) & 0xFFFFFFFF
    val high = getBlock32(data, 8 * i + 4) & 0xFFFFFFFF
    low | high << 32
  }
  def murmurhash3_x86_32(data: Array[Byte], len: Int, seed: Int): Int = {
    val nblocks = len / 4
    var h1 = seed
    val c1 = -862048943
    val c2 = 461845907

    for (i <- 0 until nblocks) {
      var k1 = getBlock32(data, 4 * i)
      k1 *= c1
      k1 = k1 << 15 | k1 >>> 17
      k1 *= c2
      h1 ^= k1
      h1 = h1 << 13 | h1 >>> 19
      h1 = h1 * 5 + -430675100
    }
    val tail = 4 * nblocks
    var k1 = 0
    len & 0x3 match {
      case 3 =>
        k1 ^= unsignedByte(data(tail + 2)) << 16
      case 2 =>
        k1 ^= unsignedByte(data(tail + 1)) << 8
      case 1 =>
        k1 ^= unsignedByte(data(tail))
        k1 *= c1
        k1 = k1 << 15 | k1 >>> 17
        k1 *= c2
        h1 ^= k1
    }
    h1 ^= len
    var h = h1
    h ^= h >>> 16
    h *= -2048144789
    h ^= h >>> 13
    h *= -1028477387
    h ^= h >>> 16
    h1 = h
    h1
  }

  private def fmix64(k: Long): Long = {
    var x: Long = k ^ ( k >>> 33)
    x *= -49064778989728563L
    x ^= (x >>> 33)
    x *= -4265267296055464877L
    x ^= (x >>> 33)
    x
  }

  def murmurhash3_x64_64(data: Array[Byte], len: Int, seed: Int): Long = {
    val nblocks = len / 16
    var h1: Long = seed & 0xFFFFFFFF
    var h2 = h1
    val c1 = -8663945395140668459L
    val c2 = 5545529020109919103L

    for (i <- 0 until nblocks) {
      var k1 = getBlock64(data, 2 * i)
      var k2 = getBlock64(data, 2 * i + 1)
      k1 *= -8663945395140668459L
      k1 = ROTL64(k1, 31)
      k1 *= 5545529020109919103L
      // h1 ^= k1
      h1 =  h1 ^ k1
      h1 = ROTL64(h1, 27)
      h1 += h2
      h1 = h1 * 5L + 1390208809L
      k2 *= 5545529020109919103L
      k2 = ROTL64(k2, 33)
      k2 *= -8663945395140668459L
      h2 ^= k2
      h2 = ROTL64(h2, 31)
      h2 += h1
      h2 = h2 * 5L + 944331445L
    }
    val tail = 16 * nblocks
    var k1 = 0L
    var k2 = 0L
    len & 0xF match {
      case 15 =>
        k2 ^= unsignedByte(data(tail + 14)) << 48
      case 14 =>
        k2 ^= unsignedByte(data(tail + 13)) << 40
      case 13 =>
        k2 ^= unsignedByte(data(tail + 12)) << 32
      case 12 =>
        k2 ^= unsignedByte(data(tail + 11)) << 24
      case 11 =>
        k2 ^= unsignedByte(data(tail + 10)) << 16
      case 10 =>
        k2 ^= unsignedByte(data(tail + 9)) << 8
      case 9 =>
        k2 ^= unsignedByte(data(tail + 8)) << 0
        k2 *= 5545529020109919103L
        k2 = ROTL64(k2, 33)
        k2 *= -8663945395140668459L
        h2 ^= k2
      case 8 =>
        k1 ^= unsignedByte(data(tail + 7)) << 56
      case 7 =>
        k1 ^= unsignedByte(data(tail + 6)) << 48
      case 6 =>
        k1 ^= unsignedByte(data(tail + 5)) << 40
      case 5 =>
        k1 ^= unsignedByte(data(tail + 4)) << 32
      case 4 =>
        k1 ^= unsignedByte(data(tail + 3)) << 24
      case 3 =>
        k1 ^= unsignedByte(data(tail + 2)) << 16
      case 2 =>
        k1 ^= unsignedByte(data(tail + 1)) << 8
      case 1 =>
        k1 ^= unsignedByte(data(tail + 0)) << 0
        k1 *= -8663945395140668459L
        k1 = ROTL64(k1, 31)
        k1 *= 5545529020109919103L
        h1 ^= k1
    }
    h1 ^= len
    h2 ^= len
    h1 += h2
    h2 += h1
    h1 = fmix64(h1)
    h2 = fmix64(h2)
    h1 += h2
    h2 += h1
    h1
  }

  private def ROTL64(x: Long, r: Int): Long = {
    x << r | x >>> 64 - r
  }

  def murmurhash3_x64_32(key: Array[Byte], len: Int, seed: Int): Int = {
    (murmurhash3_x64_64(key, len, seed) >>> 32).asInstanceOf[Int]
  }

  def main(args: Array[String]): Unit = {
    val s = "123abc"
    val v = murmurhash3_x64_64(s.getBytes, s.length, 0)
    println(s"hash: ${v}")
  }
}
