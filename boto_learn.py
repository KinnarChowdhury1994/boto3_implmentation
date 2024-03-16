from flask import Flask,request,jsonify
# from datetime import datetime
from utils import setup_logger,list_s3_bucket,create_s3_bucket

log = setup_logger('LOG', 'boto.log')

#! Creating Server
app = Flask(__name__)

@app.route("/")
def hello():
    return "<H1>Your Application is Running Successfully</H1>"

@app.route("/s3/", methods=['GET','POST'])
def s3():
    try:
        if request.method == 'GET':
            # return "<H1>GET METHOD</H1>"
            
            list_s3 = list_s3_bucket()
            log.warning(f'S3 Buckets List Data :\n{list_s3}')
            
            response = {}
            response['Buckets'] = list_s3['Buckets']
            response = jsonify(response)
        
        if request.method == 'POST':
            # log.error(request)
            # log.error(request.json)
            data = request.json
            log.info(f'Payload -> {data}')
            bucket_name = data['bucket_name'] if data.get('bucket_name') else None
            
            if bucket_name in (None,''):
                raise Exception('Please Provide Bucket Name')
            
            create_s3 = create_s3_bucket(bucket_name=bucket_name)
            
            response = {}
            response['create'] = create_s3
            response = jsonify(response)
    except Exception as e:
        log.exception(e)
        response = {"Data": None, "Exception":True,"Message":"Error Encountered. Please Try Again."}
        response = jsonify(response)
        
    finally:
        return response
    
if (__name__ == "__main__"):
    while True:
        app.run(debug=True)