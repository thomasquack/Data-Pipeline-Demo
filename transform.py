import spacy
import json
import boto3
from datetime import datetime

def lambda_handler(event, context):
    # get the json file from S3
    bucketname = event['Records'][0]['s3']['bucket']['name']
    filename = event['Records'][0]['s3']['object']['key']
    
    s3 = boto3.resource('s3')
    obj = s3.Object(bucketname, filename)
    data = json.loads(obj.get()['Body'].read().decode('utf-8'))

    # load nlp model from spacy
    nlp = spacy.load("/opt/en_core_web_sm-2.2.5/en_core_web_sm/en_core_web_sm-2.2.5")

    entities = {}
    for text in data:
        doc = nlp(text)
        for ent in doc.ents:
            if ent.label_ not in ['DATE', 'TIME', 'ORDINAL', 'CARDINAL', 'MONEY']:
                if ent.text not in entities.keys():
                    entities[ent.text] = 1
                else:
                    entities[ent.text] += 1
    
    print([x for x in sorted(entities.items(), key = lambda x: x[1], reverse = True)])

    # dump word count to s3    
    date = datetime.now().strftime('%d%m%Y-%H%M')
    s3object = s3.Object('dataengineeringwordcount1239345', f'ner_count_{date}.json')
    s3object.put(
        Body = json.dumps(entities).encode('UTF-8')
    )

    return {
        'statusCode': 200,
    }
