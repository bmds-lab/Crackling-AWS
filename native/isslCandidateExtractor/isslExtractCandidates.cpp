/*
Hydrates a batch of compressed ISSL buckets for one shared, half-open global
off-target ID interval [startId, endId). The Lambda wrapper stages the inputs
and invokes this process once per partition; buckets are processed in plan order.

CLI (eight positional arguments):
  extractor signatures.bin occurrences.bin overflow.bin buckets.tsv
    startId endId idBits totalCount

Input contracts (little-endian):
  signatures.bin:  headerless 5-byte signatures for [startId, endId), ID order
  occurrences.bin: one byte per partition ID; 1..254 is the full count,
                   255 selects a full uint32 count from overflow.bin; 0 is invalid
  overflow.bin:    partition-only rows, sorted by global ID, packed as <QI4x>
                   (uint64 globalId, uint32 occurrences >= 255, 4 padding bytes)
  buckets.tsv:     nonempty, headerless LF-delimited plan; each row has exactly
                   bucketPath<TAB>elementCount<TAB>outputPath
                   elementCount is the complete bucket's decoded entry count
  Each bucket is a headerless unsigned LEB128 stream: first value is an absolute
  global ID; subsequent values are positive deltas, giving strictly increasing
  IDs below totalCount. Decoding stops at the first ID >= endId; an unvisited
  suffix is not validated. If fully scanned, count and byte length must match.
  All numeric CLI arguments are unsigned decimal integers, with
  0 <= startId <= endId <= totalCount <= 2^40. idBits must be 32 when
  totalCount <= 2^32 and 64 otherwise.

Output contract:
  Each plan row truncates/creates its own headerless, packed little-endian file.
  One record is written per bucket ID in [startId, endId), in bucket order:
    idBits == 32: <QII> (uint64 signature, uint32 globalId, uint32 occurrences)
    idBits == 64: <QQI> (uint64 signature, uint64 globalId, uint32 occurrences)
  Records occupy 16 or 20 bytes. The 5-byte signature is zero-extended to uint64;
  occurrences are resolved counts, never expanded into repeated records.
  A bucket with no matching IDs produces an empty output file.
  Success returns 0 and prints candidateRecords=<sum across all outputs>.
  Failures return 1 with diagnostics on stderr; partial outputs may remain.

Build from the Crackling-AWS repository root in the Lambda-compatible container:
  g++ -o layers/isslCandidateExtractor/extractor \
    native/isslCandidateExtractor/isslExtractCandidates.cpp \
    -O3 -std=c++11 -static-libgcc -static-libstdc++
  See layers/isslCandidateExtractor/README.md for the Docker build command.
*/

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstddef>
#include <stdexcept>
#include <fstream>
#include <iostream>
#include <limits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string>

static bool parseUint64(const char *text, uint64_t &value) {
    if (text == nullptr || *text == '\0') return false;
    for (const char *p = text; *p != '\0'; ++p) {
        if (*p < '0' || *p > '9') return false;
    }
    errno = 0;
    char *end = nullptr;
    unsigned long long parsed = std::strtoull(text, &end, 10);
    if (errno != 0 || *end != '\0' ||
        parsed > std::numeric_limits<uint64_t>::max()) return false;
    value = static_cast<uint64_t>(parsed);
    return true;
}


static bool fileSize(FILE *file, size_t &size) {
    struct stat statBuffer;
    if (fstat(fileno(file), &statBuffer) != 0 || statBuffer.st_size < 0) return false;
    size = static_cast<size_t>(statBuffer.st_size);
    return true;
}

static uint64_t readLittleEndian(const uint8_t *data, unsigned bytes) {
    uint64_t value = 0;
    for (unsigned i = 0; i < bytes; ++i)
        value |= uint64_t(data[i]) << (8 * i);
    return value;
}

static void validateOccurrences(
    const uint8_t *occurrences, const uint8_t *overflow,
    size_t overflowBytes, uint64_t startId, uint64_t endId) {
    const size_t count = overflowBytes / 16;
    size_t row = 0;
    for (uint64_t globalId = startId; globalId < endId; ++globalId) {
        const uint8_t b = occurrences[static_cast<size_t>(globalId - startId)];
        if (b == 0)
            throw std::runtime_error("Zero occurrence count");
        if (b != 0xff) continue;
        if (row == count)
            throw std::runtime_error("Missing overflow row");
        const uint8_t *entry = overflow + 16 * row;
        if (readLittleEndian(entry, 8) != globalId ||
            readLittleEndian(entry + 8, 4) < 255)
            throw std::runtime_error("Invalid overflow row/order/count");
        ++row;
    }
    if (row != count)
        throw std::runtime_error("Unexpected overflow rows");
}

static uint32_t getOccurrences(
    uint64_t globalId, uint64_t startId, uint64_t endId,
    const uint8_t *occurrences, const uint8_t *overflow,
    size_t overflowBytes) {
    if (globalId < startId || globalId >= endId)
        throw std::runtime_error("Occurrence ID outside partition");
    const uint8_t b = occurrences[static_cast<size_t>(globalId - startId)];
    if (b != 0xff) return b;
    size_t lo = 0, hi = overflowBytes / 16;
    while (lo < hi) {
        const size_t mid = lo + (hi - lo) / 2;
        if (readLittleEndian(overflow + 16 * mid, 8) < globalId)
            lo = mid + 1;
        else
            hi = mid;
    }
    if (lo == overflowBytes / 16 ||
        readLittleEndian(overflow + 16 * lo, 8) != globalId)
        throw std::runtime_error("Missing overflow occurrence");
    return static_cast<uint32_t>(
        readLittleEndian(overflow + 16 * lo + 8, 4));
}

static void storeLittleEndian(uint8_t *data, uint64_t value, unsigned bytes) {
    for (unsigned i = 0; i < bytes; ++i)
        data[i] = static_cast<uint8_t>(value >> (8 * i));
}

static void writeCandidate(
    std::ostream &output, uint64_t signature, uint64_t globalId,
    uint32_t occurrence, uint64_t idBits) {
    const unsigned idBytes = idBits == 64 ? 8 : 4;
    uint8_t record[20];
    storeLittleEndian(record, signature, 8);
    storeLittleEndian(record + 8, globalId, idBytes);
    storeLittleEndian(record + 8 + idBytes, occurrence, 4);
    output.write(reinterpret_cast<const char *>(record), 12 + idBytes);
}


static uint64_t decodeLeb128(
    const uint8_t *data, size_t size, size_t &offset) {
    uint64_t value = 0;
    for (unsigned byteIndex = 0; byteIndex < 10; ++byteIndex) {
        if (offset >= size)
            throw std::runtime_error("Truncated LEB128 ID");
        const uint8_t byte = data[offset++];
        const uint8_t payload = byte & 0x7f;
        if (byteIndex == 9 && (payload > 1 || (byte & 0x80)))
            throw std::runtime_error("Overflowing LEB128 ID");
        value |= uint64_t(payload) << (7 * byteIndex);
        if ((byte & 0x80) == 0) {
            if (byteIndex != 0 && payload == 0)
                throw std::runtime_error("Overlong LEB128 ID");
            return value;
        }
    }
    throw std::runtime_error("Invalid LEB128 ID");
}

static bool mapReadOnly(
    FILE *file, size_t bytes, const uint8_t *&data, void *&mapping) {
    data = nullptr;
    mapping = nullptr;

    if (bytes == 0)
        return true;

    mapping = mmap(
        nullptr, bytes, PROT_READ, MAP_PRIVATE, fileno(file), 0);

    if (mapping == MAP_FAILED) {
        mapping = nullptr;
        return false;
    }

    data = static_cast<const uint8_t *>(mapping);
    return true;
}

static void unmapReadOnly(void *mapping, size_t bytes) {
    if (mapping != nullptr && bytes != 0)
        munmap(mapping, bytes);
}


int main(int argc, char **argv) {
    if (argc != 9) {
        std::fprintf(stderr,
            "Usage: %s <signatures.bin> <occurrences.bin> <overflow.bin> <buckets.tsv> <start-id> <end-id> <id-bits> <total-count>\n",
            argv[0]);
        return 1;
    }

    uint64_t startId = 0;
    uint64_t endId = 0;
    uint64_t idBits = 0;
    uint64_t totalCount = 0;

    if (!parseUint64(argv[5], startId) ||
        !parseUint64(argv[6], endId) ||
        !parseUint64(argv[7], idBits) ||
        !parseUint64(argv[8], totalCount) ||
        totalCount > (1ULL << 40) ||
        startId > endId ||
        endId > totalCount ||
        idBits != (totalCount > (1ULL << 32) ? 64ULL : 32ULL)) {
        std::fprintf(stderr, "Invalid extractor arguments\n");
        return 1;
    }

    FILE *signatureFile = std::fopen(argv[1], "rb");
    FILE *occurrenceFile = std::fopen(argv[2], "rb");
    FILE *overflowFile = std::fopen(argv[3], "rb");

    if (!signatureFile || !occurrenceFile || !overflowFile) {
        std::fprintf(stderr, "Unable to open extractor input file\n");
        // Process exit releases any input files that were opened successfully.
        return 1;
    }

    const uint64_t partitionRecords = endId - startId;
    size_t signatureBytes = 0;
    size_t occurrenceBytes = 0;
    size_t overflowBytes = 0;

    if (!fileSize(signatureFile, signatureBytes) ||
        !fileSize(occurrenceFile, occurrenceBytes) ||
        !fileSize(overflowFile, overflowBytes) ||
        signatureBytes != partitionRecords * 5 ||
        occurrenceBytes != partitionRecords ||
        overflowBytes % 16 != 0 ||
        overflowBytes / 16 > partitionRecords) {
        std::fprintf(stderr, "Extractor input sizes violate contract\n");
        std::fclose(signatureFile);
        std::fclose(occurrenceFile);
        std::fclose(overflowFile);
        return 1;
    }

    // Keep the three catalogue partitions mapped across all bucket plan rows.
    const uint8_t *signatures = nullptr;
    const uint8_t *occurrences = nullptr;
    const uint8_t *overflow = nullptr;

    void *signatureMapping = nullptr;
    void *occurrenceMapping = nullptr;
    void *overflowMapping = nullptr;

    if (!mapReadOnly(signatureFile, signatureBytes,
                    signatures, signatureMapping) ||
        !mapReadOnly(occurrenceFile, occurrenceBytes,
                    occurrences, occurrenceMapping) ||
        !mapReadOnly(overflowFile, overflowBytes,
                    overflow, overflowMapping)){
        std::fprintf(stderr, "Unable to memory-map extractor input\n");

        unmapReadOnly(signatureMapping, signatureBytes);
        unmapReadOnly(occurrenceMapping, occurrenceBytes);
        unmapReadOnly(overflowMapping, overflowBytes);

        std::fclose(signatureFile);
        std::fclose(occurrenceFile);
        std::fclose(overflowFile);
        return 1;
    }

    FILE *bucketFile = nullptr;
    void *bucketMapping = nullptr;
    size_t bucketBytes = 0;

    try{
    std::ifstream plan(argv[4]);
        if (!plan)
            throw std::runtime_error("Unable to open bucket plan");

        validateOccurrences(occurrences, overflow, overflowBytes, startId, endId);

        std::string row;
        uint64_t totalWritten = 0;
        size_t rowCount = 0;

        while (std::getline(plan, row)) {
            const size_t firstTab = row.find('\t');
            const size_t secondTab = firstTab == std::string::npos
                ? std::string::npos : row.find('\t', firstTab + 1);

            if (firstTab == std::string::npos ||
                secondTab == std::string::npos ||
                row.find('\t', secondTab + 1) != std::string::npos)
                throw std::runtime_error("Invalid bucket plan row");

            const std::string bucketPath = row.substr(0, firstTab);
            const std::string countText =
                row.substr(firstTab + 1, secondTab - firstTab - 1);
            const std::string outputPath = row.substr(secondTab + 1);
            uint64_t elementCount = 0;

            if (bucketPath.empty() || outputPath.empty() ||
                !parseUint64(countText.c_str(), elementCount) ||
                elementCount > totalCount)
                throw std::runtime_error("Invalid bucket plan row");

            bucketFile = std::fopen(bucketPath.c_str(), "rb");
            if (!bucketFile)
                throw std::runtime_error("Unable to open bucket file");

            if (!fileSize(bucketFile, bucketBytes) ||
                (elementCount == 0 && bucketBytes != 0))
                throw std::runtime_error("Bucket size violates contract");

            const uint8_t *bucket = nullptr;
            if (!mapReadOnly(bucketFile, bucketBytes, bucket, bucketMapping))
                throw std::runtime_error("Unable to memory-map bucket");

            std::ofstream output(
                outputPath.c_str(), std::ios::binary | std::ios::trunc);
            if (!output)
                throw std::runtime_error("Unable to create candidate output");

            size_t offset = 0;
            uint64_t previousId = 0, decoded = 0, written = 0;
            bool stoppedAtUpperBound = false;

            for (uint64_t bucketEntryIndex = 0;
                bucketEntryIndex < elementCount; ++bucketEntryIndex) {
                const uint64_t encoded =
                    decodeLeb128(bucket, bucketBytes, offset);

                if (bucketEntryIndex != 0 &&
                    (encoded == 0 ||
                    encoded > std::numeric_limits<uint64_t>::max() - previousId))
                    throw std::runtime_error("Invalid or overflowing ID delta");

                const uint64_t globalId =
                    bucketEntryIndex == 0 ? encoded : previousId + encoded;

                if (globalId >= totalCount)
                    throw std::runtime_error("Global ID is outside the catalogue");

                previousId = globalId;
                ++decoded;

                if (globalId < startId) continue;
                if (globalId >= endId) {
                    stoppedAtUpperBound = true;
                    break;
                }

                const size_t localId =
                    static_cast<size_t>(globalId - startId);
                const uint64_t signature =
                    readLittleEndian(signatures + localId * 5, 5);
                const uint32_t occurrence =
                    getOccurrences(globalId, startId, endId,
                                occurrences, overflow, overflowBytes);
                writeCandidate(output, signature, globalId, occurrence, idBits);
                if (!output)
                    throw std::runtime_error(
                        "Failed while writing candidate output");
                ++written;
            }

            if (!stoppedAtUpperBound &&
                (decoded != elementCount || offset != bucketBytes))
                throw std::runtime_error(
                    "Bucket count or byte length does not match");

            output.close();
            if (!output)
                throw std::runtime_error(
                    "Failed closing candidate output");

            unmapReadOnly(bucketMapping, bucketBytes);
            bucketMapping = nullptr;
            std::fclose(bucketFile);
            bucketFile = nullptr;
            bucketBytes = 0;

            totalWritten += written;
            ++rowCount;
        }

        if (plan.bad())
            throw std::runtime_error("Failed reading bucket plan");
        if (rowCount == 0)
            throw std::runtime_error("Empty bucket plan");

        unmapReadOnly(signatureMapping, signatureBytes);
        unmapReadOnly(occurrenceMapping, occurrenceBytes);
        unmapReadOnly(overflowMapping, overflowBytes);
        std::fclose(signatureFile);
        std::fclose(occurrenceFile);
        std::fclose(overflowFile);

        std::cout << "candidateRecords=" << totalWritten << "\n";
        return 0;

    } catch (const std::exception &error) {
    std::fprintf(stderr, "Extractor failed: %s\n", error.what());
    unmapReadOnly(bucketMapping, bucketBytes);
    if (bucketFile) std::fclose(bucketFile);
    unmapReadOnly(signatureMapping, signatureBytes);
    unmapReadOnly(occurrenceMapping, occurrenceBytes);
    unmapReadOnly(overflowMapping, overflowBytes);
    std::fclose(signatureFile);
    std::fclose(occurrenceFile);
    std::fclose(overflowFile);
    return 1;
    }
}
