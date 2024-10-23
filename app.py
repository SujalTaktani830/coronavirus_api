import os
import requests
import operator
import re
import json
from collections import Counter
from flask import Flask, render_template, request, jsonify, abort
from flask_sqlalchemy import SQLAlchemy
from stop_words import get_stop_words
from bs4 import BeautifulSoup
from rq import Queue
from rq.job import Job
from worker import conn
import nltk

app = Flask(__name__)
app.config.from_object(os.environ['APP_SETTINGS'])
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

q = Queue(connection=conn)

from models import Result  # Ensure this import is correct

nltk.data.path.append('./nltk_data/')  # Set the NLTK data path

def validate_url(url):
    if not url.startswith(('http://', 'https://')):
        return 'http://' + url
    return url

def count_and_save_words(url):
    errors = []

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses
    except requests.RequestException as e:
        errors.append(str(e))
        return {"error": errors}

    # Text processing
    raw = BeautifulSoup(response.text, 'html.parser').get_text()
    tokens = nltk.word_tokenize(raw)
    raw_words = [w for w in tokens if re.match(r'.*[A-Za-z].*', w)]
    
    raw_word_count = Counter(raw_words)
    stops = set(get_stop_words("en"))
    no_stop_words = [w for w in raw_words if w.lower() not in stops]
    no_stop_words_count = Counter(no_stop_words)

    # Save results
    try:
        result = Result(url=url, result_all=raw_word_count, result_no_stop_words=no_stop_words_count)
        db.session.add(result)
        db.session.commit()
        return result.id
    except Exception as e:
        errors.append("Unable to add item to database: {}".format(e))
        return {"error": errors}

@app.route('/', methods=['GET', 'POST'])
def index():
    results = {}
    if request.method == "POST":
        url = request.form['url']
        url = validate_url(url)
        job = q.enqueue(count_and_save_words, url)
        print(job.get_id())  # Optional: handle the job ID if needed
    return render_template('index.html', results=results)

@app.route('/start', methods=['POST'])
def get_counts():
    data = request.get_json()
    url = validate_url(data.get("url", ""))
    if not url:
        abort(400, "Invalid URL provided.")
    
    job = q.enqueue(count_and_save_words, url)
    return jsonify(job_id=job.get_id()), 202

@app.route('/results/<job_key>', methods=['GET'])
def get_job(job_key):
    job = Job.fetch(job_key, connection=conn)
    if job.is_finished:
        result = Result.query.filter_by(id=job.result).first()
        results = sorted(result.result_no_stop_words.items(), key=operator.itemgetter(1), reverse=True)[:10]
        return jsonify(results), 200
    else:
        return jsonify(status="Job is still running."), 202

if __name__ == '__main__':
    app.run()
