#!/var/lang/bin/python3
'''
Faster and better CRISPR guide RNA design with the Crackling method.
Jacob Bradford, Timothy Chappell, Dimitri Perrin
bioRxiv 2020.02.14.950261; doi: https://doi.org/10.1101/2020.02.14.950261


Purpose:    identify all offtarget sites in the whole genome

Input:      FASTA, or multi-FASTA, formatted file

Output:     one file with all the sites

To use:     python3.7 ExtractOfftargets.py output-file  (input-files... | input-dir>)

''' 
import glob, os, re, shutil, sys, tempfile, heapq, argparse

SEQ_LEN = 20
BYTES_PER_SEQ = (SEQ_LEN * 2 + 7) // 8

# Defining the patterns used to detect sequences
pattern_forward_offsite = r"(?=([ACG][ACGT]{19}[ACGT][AG]G))"
pattern_reverse_offsite = r"(?=(C[CT][ACGT][ACGT]{19}[TGC]))"

nucleotideIndex = {
    'A': 0,
    'C': 1,
    'G': 2,
    'T': 3
}

# Function that returns the reverse-complement of a given sequence
def rc(dna):
    complements = str.maketrans('acgtrymkbdhvACGTRYMKBDHV', 'tgcayrkmvhdbTGCAYRKMVHDB')
    rcseq = dna.translate(complements)[::-1]
    return rcseq

def encode_sequence(seq: str) -> bytes:
    value = 0
    for base in seq:
        value = (value << 2) | nucleotideIndex[base]
    num_bytes = (len(seq) * 2 + 7) // 8
    return value.to_bytes(num_bytes, byteorder="big")

def record_reader(fp):
    while True:
        chunk = fp.read(BYTES_PER_SEQ)
        if not chunk:
            break
        yield (int.from_bytes(chunk, "big"), chunk)
    
def explodeMultiFastaFile(fpInput, fpOutputTempDir, Invocation, totalInvocations):
    newFilesPaths = []
    currentHeaderIndex = 0
    workerID = Invocation - 1
    fWrite = None

    with open(fpInput, 'r') as fRead:
        for line in fRead:
            line = line.strip()
            if line.startswith('>'):
                # Close previous file if open
                if fWrite is not None:
                    fWrite.close()
                    fWrite = None

                # Check if this header belongs to this invocation
                if currentHeaderIndex % totalInvocations == workerID:
                    # Create new temp file
                    fpTemp = tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=fpOutputTempDir)
                    fWrite = fpTemp
                    newFilesPaths.append(fpTemp.name)
                    fWrite.write(line + '\n')

                currentHeaderIndex += 1
            else:
                if fWrite is not None:
                    fWrite.write(line.upper() + '\n')

    if fWrite is not None:
        fWrite.close()

    return newFilesPaths

def processingNode(fpInput, fpOutputTempDir = None):
    # Create a temporary file
    fpTemp = tempfile.NamedTemporaryFile(
        mode = 'wb+', 
        delete = False,
        dir = fpOutputTempDir
    )

    with open(fpTemp.name, 'wb') as outFile:
        # key: FASTA header, value: sequence
        seqsByHeader = {}

        with open(fpInput, 'r') as inFile:
            header = fpInput
            
            for line in inFile:
                if line[0] == '>':
                    header = line[1:]
                    seqsByHeader[header] = []
                else:
                    if header not in seqsByHeader:
                        # it could be a plain text file, without a header
                        seqsByHeader[header] = []
                    seqsByHeader[header].append(line.rstrip().upper())

        # For each FASTA sequence
        offTargetCount = 0
        for header in seqsByHeader:
            seq = ''.join(seqsByHeader[header])
            
            for strand, pattern, seqModifier in [
                ['positive', pattern_forward_offsite, lambda x : x],
                ['negative', pattern_reverse_offsite, lambda x : rc(x)]
            ]:
                match_chr = re.findall(pattern, seq)

                for i in range(0,len(match_chr)):
                    offTarget = seqModifier(match_chr[i][0:20])
                    outFile.write(encode_sequence(offTarget))
                    offTargetCount += 1       
        return offTargetCount

# Node function that sorts a file for multiprocessing pool
def sortingNode(fileToSort, sortedTempDir):
    # Create a temporary file
    sortedFile = tempfile.NamedTemporaryFile(
        mode = 'wb', 
        delete = False,
        dir = sortedTempDir
    )
    # Sort input file and store in new output dir
    with open(fileToSort, 'rb') as input:
        # Read entire file as fixed-length records
        data = []
        while True:
            chunk = input.read(BYTES_PER_SEQ)
            if not chunk:
                break
            data.append(chunk)
        
    # Sort by raw binary value
    data.sort(key=lambda record: int.from_bytes(record, "big"))
    # Write sorted page to file
    for record in data:
        sortedFile.write(record)
    # Close sorted file
    sortedFile.close()

def paginatedSort(filesToSort, fpOutput, maxNumOpenFiles=400): 
    # Create temp file directory
    sortedTempDir = tempfile.TemporaryDirectory()
    print(f'Created temp directory {sortedTempDir.name} for sorting')

    for f in filesToSort:
        sortingNode(f,sortedTempDir.name)
    
    print('Sorting of each file completed')
    
    # Collect sorted files to merge
    sortedFiles = glob.glob(
        os.path.join(
            sortedTempDir.name,
            '*'
        )
    )
    
    # Open all the sorted files to merge
    print(f'Beginning to merge sorted files, {maxNumOpenFiles:,} at a time')
    while len(sortedFiles) > 1:
        # A file to write the merged sequences to
        mergedFile = tempfile.NamedTemporaryFile(mode='wb', delete = False)
        
        # Select the files to merge
        while True:
            try:
                sortedFilesPointers = [open(file, 'rb') for file in sortedFiles[:maxNumOpenFiles]]
                break
            except OSError as e:
                if e.errno == 24:
                    
                    for file in sortedFilesPointers:
                        try:
                            file.close()
                        except Exception as e:
                            pass

                    print(f'Attempted to open too many files at once (OSError errno 24)')
                    maxNumOpenFiles = max(1, int(maxNumOpenFiles / 2))
                    print(f'Reducing the number of files that can be opened by half to {maxNumOpenFiles}')
                    continue
                raise e
                    
        print(f'Merging {len(sortedFilesPointers):,}')
        
        # Merge and write
        merged_iter = heapq.merge(*(record_reader(file) for file in sortedFilesPointers))
        for _, record in merged_iter:
            mergedFile.write(record)
        
        # Close all of the open files
        for file in sortedFilesPointers:
            file.close()
          
        # prepare for the next set to be merged
        sortedFiles = sortedFiles[maxNumOpenFiles:] + [mergedFile.name]
    
    shutil.move(sortedFiles[0], fpOutput)

def startSequentalprocessing(fpInputs, fpOutput, numThreads, maxOpenFiles, context, Invocation, totalInvocations):
    print('Extracting off-targets using sequental-processing approach')
    
    print(f'Allowed processes: {numThreads}')
    
    fpTempDir = tempfile.TemporaryDirectory()
    print(f'Created a temporary directory for intermediate files: {fpTempDir.name}')

    if len(fpInputs) == 1 and os.path.isdir(fpInputs[0]):
        fpInputs = glob.glob(
            os.path.join(
                fpInputs[0], 
                '*'
            )
        )

    if len(fpInputs) == 1:
        print('Only one input file to process')
        
        fpExplodeTempDir = tempfile.TemporaryDirectory()
        print('Attempting to explode multi-FASTA file')
        print(f'Writing each file to a temporary directory: {fpExplodeTempDir.name}')
        
    
        fpInputs = explodeMultiFastaFile(
            fpInputs[0],
            fpExplodeTempDir.name,
            Invocation, 
            totalInvocations
        )
        
        print(f'Exploded into {len(fpInputs)} files')

    print(f'Current run-time = {900000 - context.get_remaining_time_in_millis()}ms')

    print(f'Beginning to process {len(fpInputs)} files')

    # Submit job to "multiprocessing pool"
    f = lambda x: processingNode(x,fpTempDir.name)
    numTargets = map(
        f,#processingNode,
        fpInputs#args
    )

    print(f'Processing completed. Found {sum(numTargets):,} targets.')

    print(f'Current run-time = {900000 - context.get_remaining_time_in_millis()}ms')
    
    print('Preparing for ISSL by sorting all intermediate files')
    
    # sort in batches
    paginatedSort(
        glob.glob(
            os.path.join(
                fpTempDir.name, 
                '*'
            )
        ), 
        fpOutput,
        maxNumOpenFiles=maxOpenFiles
    )

    print(f'Current run-time = {900000 - context.get_remaining_time_in_millis()}ms')

    print("end reached. Goodbye")

def main():
    parser = argparse.ArgumentParser(description='Extract CRISPR target sites for Crackling.')
    
    parser.add_argument('output',
        help='A file to write the off-targets to.'
    )
    parser.add_argument('inputs',
        help='A space-separated list of paths or a path containing wildcards to FASTA files.',
        nargs='+'
    )
    parser.add_argument('--maxOpenFiles', 
        help='The number of files allowed to be opened by a single process. Use `ulimit -n` to find out your system setting.', 
        type=int,
        default=1000, 
        required=False
    )
    parser.add_argument('--threads', 
        help='The number of threads to use. Default is `os.cpu_count()`.', 
        type=int,
        default=os.cpu_count(), 
        required=False
    )

    args = parser.parse_args()

    # Create multiprocessing pool
    # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.Pool.starmap
    # mpPool = multiprocessing.Pool(args.threads)

    startSequentalprocessing(
        args.inputs, 
        args.output, 
        args.threads, 
        args.maxOpenFiles
    )

    print('Goodbye.')

if __name__ == '__main__':
    main()