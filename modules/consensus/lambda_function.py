import json, joblib
from sklearn.svm import SVC

SGRNASCORER2_MODEL = joblib.load('/opt/model-py3.txt')

def CalcConsensus(message):
    seq = message['sequence']
    consensus = [
        _CalcChopchop(seq),
        _CalcMm10db(seq),
        _CalcSgrnascorer(seq)
    ]
    
def _CalcChopchop(seq):
    '''
    CHOPCHOP accepts guides with guanine in position 20
    '''
    return (seq[19] == 'G')
    
def _CalcMm10db(seq):
    '''
    mm10db accepts guides that:
        - do not contain poly-thymine seqs (TTTT)
        - AT% between 20-65%
        - Secondary structure energy
    '''
    
    AT = sum([c in 'AT' for c in seq])/len(seq)
    
    return all([
        'TTTT' not in seq,
        (AT >= 0.20 and AT <= 0.65),
        True
    ])
    
def _CalcSgrnascorer(seq):
    encoding = {
        'A' : '0001',        'C' : '0010',        'T' : '0100',        
        'G' : '1000',        'K' : '1100',        'M' : '0011',
        'R' : '1001',        'Y' : '0110',        'S' : '1010',        
        'W' : '0101',        'B' : '1110',        'V' : '1011',        
        'H' : '0111',        'D' : '1101',        'N' : '1111'
    }

    entryList = []

    x = 0
    while x < 20:
        y = 0
        while y < 4:
            entryList.append(int(encoding[seq[x]][y]))
            y += 1
        x += 1

    # predict based on the entry
    prediction = SGRNASCORER2_MODEL.predict([entryList])
    score = SGRNASCORER2_MODEL.decision_function([entryList])[0]

    return (float(score) >= 0)

def lambda_handler(event, context):
    print(event)
    
    for record in event['Records']:
    
        message = None
    
        try:
            if 'Sns' in record:
                if 'Message' in record['Sns']:
                    message = json.loads(record['Sns']['Message'])
        except e:
            continue

        consensus = CalcConsensus(message)
        
        print(message['sequence'], consensus)
        
    return (event)
    