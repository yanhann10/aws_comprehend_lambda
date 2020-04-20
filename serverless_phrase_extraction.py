import json
import boto3
from pprint import pprint
import logging

LOG = logging.getLogger()
LOG.setLevel(logging.DEBUG)


def read_input(bucket, key):
    """read input from s3"""
    s3 = boto3.client("s3")
    LOG.info(f"reading from {bucket}")
    file = s3.get_object(Bucket=bucket, Key=key)
    paragraph = str(file["Body"].read())
    return paragraph


def txt_preprocessing(paragraph):
    return paragraph.replace("\\n", " ")


def extract_phrase(paragraph):
    """filter for key phrase with 0.9 score or above based on AWS comprehend"""
    LOG.info("Extracting phrases")
    comprehend = boto3.client("comprehend")
    keyphrase = comprehend.detect_key_phrases(Text=paragraph, LanguageCode="en")
    LOG.debug(f"Found keyphrases: {keyphrase}")
    output_lst = [x["Text"] for x in keyphrase["KeyPhrases"] if x["Score"] > 0.9]
    LOG.info(f"output - list of key phrases: {output_lst}")
    return output_lst


def lambda_handler(event, context):
    """extract key phrases using aws comprehend"""
    bucket = "lambda-comprehend"
    key = "job_desc.txt"
    paragraph = read_input(bucket, key)
    para_processed = txt_preprocessing(paragraph)
    keyphrases = extract_phrase(para_processed)
    dynamodb = boto3.client("dynamodb")
    dynamodb.put_item(TableName="skill", Item={"guid": keyphrases})
    return keyphrases
