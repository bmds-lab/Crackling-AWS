/*

Faster and better CRISPR guide RNA design with the Crackling method.
Jacob Bradford, Timothy Chappell, Dimitri Perrin
bioRxiv 2020.02.14.950261; doi: https://doi.org/10.1101/2020.02.14.950261


To compile:

g++ -o isslCreateIndex isslCreateIndex.cpp -O3 -std=c++11 -mpopcnt

*/


#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <map>

using namespace std;

size_t seqLength;
vector<uint8_t> nucleotideIndex(256);
vector<char> signatureIndex(4);

size_t getFileSize(const char *path)
{
    struct stat64 statBuf;
    stat64(path, &statBuf);
    return statBuf.st_size;
}

uint64_t sequenceToSignature(const char *ptr)
{
    uint64_t signature = 0;
    for (size_t j = 0; j < seqLength; j++) {
        signature |= (uint64_t)(nucleotideIndex[*ptr]) << (j * 2);
        ptr++;
    }
    return signature;
}

string signatureToSequence(uint64_t signature)
{
    string sequence = string(seqLength, ' ');
    for (size_t j = 0; j < seqLength; j++) {
        sequence[j] = signatureIndex[(signature >> (j * 2)) & 0x3];
    }
    return sequence;
}

// combinations
vector<uint64_t> computeMasksTwoBit(int seqLength, int mismatches) {
	vector<uint64_t> masks;
	
	// there are more positions than mismatches
	if (mismatches < seqLength) {
		
		if (mismatches > 0) {
			// some mismatches across a long sequence
			for (auto mask : computeMasksTwoBit(seqLength - 1, mismatches -1)) {
				// orig: masks.push_back((1 << (seqLength - 1)) + mask);
				masks.push_back((1LLU << (seqLength - 1)*2) + mask);
			}
			for (auto mask : computeMasksTwoBit(seqLength - 1, mismatches)) {
				masks.push_back(mask);
			}
		} else {
			// no mismatches at all
			masks.push_back(0LLU);
		}
		
	// mismatches >= seqLength
	// every position is going to be a mismatch
	// eg: mismatches=4 and seqLength=4, we want: 10 10 10 10
	} else {
		// orig: masks.push_back((1LLU << seqLength) - 1);
		uint64_t tempMask = 0;
		for (int i = 0; i < seqLength; i++) {
			tempMask |= (1LLU << i*2);
		}
		masks.push_back(tempMask);
	}
	return masks;
}

double single_score(int* mismatch_array, int length) {
    int i;
    double T1=1.0, T2, T3, d=0.0, score;
    double M[] = {0.0, 0.0, 0.014, 0.0, 0.0, 0.395, 0.317, 0.0, 0.389, 0.079, 0.445, 0.508, 0.613, 0.851, 0.732, 0.828, 0.615, 0.804, 0.685, 0.583};

    /* 1st term */
    for(i=0; i<length; ++i)
            T1 = T1*(1.0-M[mismatch_array[i]]);

    /* 2nd term */
    if(length==1)
            d = 19.0;
    else {
            for(i=0; i<length-1; ++i)
                    d += mismatch_array[i+1]-mismatch_array[i];
            d = d/(length-1);
    }
    T2 = 1.0 / ((19.0-d)/19.0 * 4.0 + 1);

    /* 3rd term */
    T3 = 1.0 / (length*length);

    /* Total score */
    score = T1*T2*T3*100;
    return score;
}

double sscore(uint64_t xoredSignatures)
{
    int mismatch_array[20], m = 0;
    for (size_t j = 0; j < seqLength; j++) {
        if ((xoredSignatures >> (j * 2)) & 0x3) {
            mismatch_array[m++] = j;
        }
    }
    if (m == 0) return 0.0;
    return single_score(mismatch_array, m);
}

int main(int argc, char **argv)
{
    if (argc < 7) {
        fprintf(stderr, "Usage: %s [offtargetSites.txt] [sequence length] [slice width (bits)] [sissltable] [phase of issl creation] [total distinct sites] [sequence count]\n", argv[0]);
        exit(1);
    }
    int firstPhase = atoi(argv[5]); // 1 for True, 0 for False
    size_t fileSize;
    FILE *fp = NULL;
    
    if (firstPhase == 1) {
        fileSize = getFileSize(argv[1]);
        fp = fopen(argv[1], "rb");
        if (!fp) {
            perror("Error opening file");
            exit(1);
        }
    }
    
    
    seqLength = atoi(argv[2]);
    if (seqLength > 32) {
        fprintf(stderr, "Sequence length is greater than 32, which is the maximum supported currently\n");
        exit(1);
    }
    size_t bytesPerSeq = (seqLength * 2 + 7) / 8; // 20 nt -> 5 bytes
    if (fileSize % bytesPerSeq != 0 && firstPhase == 1) {
        fprintf(stderr, "fileSize: %zu\n", fileSize);
        fprintf(stderr, "Error: file does is not a multiple of the expected line length (%zu)\n", bytesPerSeq);
        fprintf(stderr, "The sequence length may be incorrect; alternatively, the line endings\n");
        fprintf(stderr, "may be something other than LF, or there may be junk at the end of the file.\n");
        exit(1);
    }
    size_t seqCount = 0;
    if (firstPhase == 1) {
        seqCount = fileSize / bytesPerSeq;
    }
    else {
        seqCount = atoi(argv[7]);
    }
    fprintf(stderr, "Number of sequences: %zu\n", seqCount);
    
    nucleotideIndex['A'] = 0;
    nucleotideIndex['C'] = 1;
    nucleotideIndex['G'] = 2;
    nucleotideIndex['T'] = 3;
    signatureIndex[0] = 'A';
    signatureIndex[1] = 'C';
    signatureIndex[2] = 'G';
    signatureIndex[3] = 'T';
    
    vector<uint64_t> seqSignatures;
    vector<uint32_t> seqSignaturesOccurrences;

    size_t distinctSites = atoi(argv[6]);
    
    // Bring signatures back into memory if second phase
    if (distinctSites != 0) {
        fprintf(stderr, "Continuing from previous invocation\n");

        seqSignatures.resize(distinctSites);
        seqSignaturesOccurrences.resize(distinctSites);
        
        FILE *fp2 = fopen(argv[4], "rb");
        if (!fp2) {
            fprintf(stderr, "Failed to open file %s\n", argv[4]);
            exit(1);
        }
        fread(seqSignatures.data(), sizeof(uint64_t), distinctSites, fp2);
        fread(seqSignaturesOccurrences.data(), sizeof(uint32_t), distinctSites, fp2);
		fclose(fp2);
    }

    char buffer[bytesPerSeq];
    char bufferForward[bytesPerSeq];
    {
		size_t progressCount = 0;
		while (progressCount < seqCount && firstPhase == 1) {
			if (fread(buffer, bytesPerSeq, 1, fp) < 1) {
                fprintf(stderr, "Failed to read at sequence %zu\n", progressCount);
                break;
            }
			
			uint64_t signature = 0;
            for (size_t i = 0; i < bytesPerSeq; i++) {
                signature |= (uint64_t)(uint8_t)buffer[i] << (8 * (bytesPerSeq - 1 - i));
            }

            uint64_t reversedSig = 0;
            for (size_t j = 0; j < seqLength; ++j) {
                uint64_t twoBits = (signature >> (j * 2)) & 0x3;
                reversedSig |= twoBits << ((seqLength - 1 - j) * 2);
            }

            signature = reversedSig;

			// check how many times the off-target appears
			// (assumed the list is sorted)
			uint32_t occurrences = 1;
			while (fread(bufferForward, bytesPerSeq, 1, fp) == 1) {
				if (memcmp(buffer, bufferForward, bytesPerSeq) == 0) {
                    occurrences++;
				    // if ((seqCount - progressCount - occurrences) < 100)
					    // fprintf(stderr, "%zu/%zu : %zu\n", (progressCount+occurrences), seqCount, distinctSites);
                }
				else {
                    fseek(fp, -bytesPerSeq, SEEK_CUR);
                    // if (progressCount < 500) {
                    //     std::string seq = signatureToSequence(signature);
                    //     printf("%s occurences: %zu\n", seq.c_str(), occurrences);
                    // } 
                    break;
                }	
			}

            seqSignatures.push_back(signature);
            seqSignaturesOccurrences.push_back(occurrences);
			
			distinctSites++;
			// if (progressCount % 10000 == 0)
				// fprintf(stderr, "%zu/%zu : %zu\n", progressCount, seqCount, distinctSites);
			
			progressCount += occurrences;
		}
        if (fp != NULL) {
            fclose(fp);
            fp = NULL;
        }

        if (firstPhase == 1) {
            fp = fopen(argv[4], "wb");

            fwrite(seqSignatures.data(), sizeof(uint64_t), seqSignatures.size(), fp);
            fwrite(seqSignaturesOccurrences.data(), sizeof(uint32_t), seqSignaturesOccurrences.size(), fp);

            fclose(fp);

            printf("SEQCOUNT=%zu\n", seqCount);
            printf("SITES=%zu\n", distinctSites);

            return 1;
        }
    }

	printf("Finished counting occurrences, now precalculating scores...\n");
    size_t sliceWidth = atoi(argv[3]);
    size_t sliceLimit = 1 << sliceWidth;
    size_t sliceCount = (seqLength * 2) / sliceWidth;
    size_t offtargetsCount = distinctSites;

    // Precalculate all the scores
	map<uint64_t, double> precalculatedScores;
	
	int maxDist = seqLength * 2 / sliceWidth - 1;
	size_t scoresCount = 0;
	
	for (int i = 1; i <= maxDist; i++) {
		vector<uint64_t> tempMasks;
		tempMasks = computeMasksTwoBit(20, i);
		for (auto mask : tempMasks) {
			double score = sscore(mask);
			precalculatedScores.insert(pair<uint64_t, double>(mask, score));
			scoresCount++;
		}
	}

    fp = fopen(argv[4], "wb");
    vector<size_t> slicelistHeader;
    slicelistHeader.push_back(offtargetsCount);
    slicelistHeader.push_back(seqLength);
    slicelistHeader.push_back(seqCount);
    slicelistHeader.push_back(sliceWidth);
    slicelistHeader.push_back(sliceCount);
    slicelistHeader.push_back(scoresCount);
	
	
	// write the header
    fwrite(slicelistHeader.data(), sizeof(size_t), slicelistHeader.size(), fp);

	// write the precalculated scores
	for (auto const& x : precalculatedScores) {
		fwrite(&x.first, sizeof(uint64_t), 1, fp);
		fwrite(&x.second, sizeof(double), 1, fp);
	}

    fwrite(seqSignatures.data(), sizeof(uint64_t), seqSignatures.size(), fp);

    printf("Finished calculating scores, now constructing index...\n");
    
    long sizesPos = ftell(fp);

    // Reserve space for all slice sizes
    size_t zero = 0;
    for (size_t i = 0; i < sliceCount; i++) {
        for (size_t j = 0; j < sliceLimit; j++) {
            fwrite(&zero, sizeof(size_t), 1, fp);
        }
    }

    for (size_t i = 0; i < sliceCount; i++) {
        vector<vector<uint64_t>> sliceList(sliceLimit);

        uint64_t sliceMask = sliceLimit - 1;
        int sliceShift = sliceWidth * i;
        sliceMask = sliceMask << sliceShift;
        
		uint32_t signatureId = 0;
        for (uint64_t signature : seqSignatures) {
			uint32_t occurrences = seqSignaturesOccurrences[signatureId];
            uint8_t sliceVal = (signature & sliceMask) >> sliceShift;
			
			uint64_t seqSigIdVal = (((uint64_t)occurrences) << 32) | (uint64_t)signatureId;
			sliceList[sliceVal].push_back(seqSigIdVal);
			signatureId++;
        }
	
        for (size_t j = 0; j < sliceLimit; j++) { // 256
            size_t sz = sliceList[j].size();
            long sizePos = sizesPos + ((i * sliceLimit + j) * sizeof(size_t));
            long curPos = ftell(fp);

            // Write slice size
            fseek(fp, sizePos, SEEK_SET);
            fwrite(&sz, sizeof(size_t), 1, fp);

            // Write slice data
            fseek(fp, curPos, SEEK_SET);
            fwrite(sliceList[j].data(), sizeof(uint64_t), sliceList[j].size(), fp);
        }
    }

    fclose(fp);

	printf("Finished constructing index\n");

    printf("Program complete.\n");
    return 0;
}
