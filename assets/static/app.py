from flask import Flask, render_template, request, jsonify
import pandas as pd

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process_data', methods=['POST'])
def process_data():
    uploaded_file = request.files['file']
    
    # Example: Process uploaded file using Pandas
    if uploaded_file.filename.endswith('.csv'):
        df = pd.read_csv(uploaded_file)
        processed_data = df.head().to_html()
    elif uploaded_file.filename.endswith('.json'):
        df = pd.read_json(uploaded_file)
        processed_data = df.head().to_html()
    else:
        processed_data = "Unsupported file format"

    return processed_data

if __name__ == '__main__':
    app.run(debug=True)
