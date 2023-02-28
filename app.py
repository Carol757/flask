from distutils.log import debug
from fileinput import filename
import pandas as pd
import csv, json
from flask import *
from datetime import datetime
from sqlalchemy import create_engine
import os
from script import process_csv
from werkzeug.utils import secure_filename

UPLOAD_FOLDER = os.path.join('staticFiles', 'uploads')

csv_rows = []
with open('staticFiles/uploads/file1.csv', "r") as o:
    csv_file = csv.reader(o, delimiter=",")
    for row in csv_file:
        csv_rows.append(row)
    o.close()

app = Flask(__name__)

# Define allowed files
ALLOWED_EXTENSIONS = {'csv'}

app = Flask(__name__)

# Configure upload file path flask
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

app.secret_key = 'This is your secret key to utilize session in Flask'


@app.route('/', methods=['GET', 'POST'])
def uploadFile():
	if request.method == 'POST':
	# upload file flask
		f = request.files.get('file')

		# Extracting uploaded file name
		data_filename = secure_filename(f.filename)

		f.save(os.path.join(app.config['UPLOAD_FOLDER'],
							data_filename))

		session['uploaded_data_file_path'] =os.path.join(app.config['UPLOAD_FOLDER'],data_filename)

		return render_template('index2.html')
	return render_template("index.html")


@app.route('/show_data')
def showData():
	# Uploaded File Path
	data_file_path = session.get('uploaded_data_file_path', None)
	# read csv
	uploaded_df = pd.read_csv(data_file_path,
							encoding='unicode_escape')
	# Converting to html Table
	uploaded_df_html = uploaded_df.to_html()
	return render_template('show_csv_data.html',
						data_var=uploaded_df_html)

@app.route('/get_report')
def download():
    return render_template('download.html', files=os.listdir('output'))

@app.route('/get_report/<filename>')
def download_file(filename):
    filename=filename.values.tolist()
    return render_template('download.html', files=os.listdir(filename))

@app.route('/trigger_report' , methods=['GET', 'POST'])
def get_data():
    csv_data
    values = range(2)
    for i in values:
        data_file_path = session.get('uploaded_data_file_path', None)
        globals()[f"df{i}"] = pd.read_csv(data_file_path,encoding='unicode_escape')
    joined_df = pd.merge(globals()[f"df{1}"], pd.merge(globals()[f"df{2}"], globals()[f"df{3}"], on='store_id'), on='store_id')
    if request.method == 'POST':
        filename = "report"
        new_filename = f'{filename.split(".")[0]}_{str(datetime.now())}.csv'
        save_location = os.path.join('input', new_filename)
        file.save(save_location)
        output_file = process_csv(save_location)
    csv_data=output_file
    return render_template('download.html')

if __name__ == '__main__':
	app.run(debug=True)
