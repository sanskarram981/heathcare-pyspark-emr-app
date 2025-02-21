import argparse

from heathcare_data_processor import process

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="spark-emr-job-s3-bucket-arguments")
    parser.add_argument("--input", required=True, help="input-s3-bucket-url")
    parser.add_argument("--output", required=True, help="output-s3-bucket-url")
    args = parser.parse_args()

    process(args.input, args.output)
