from flask import Flask, send_file

app = Flask(__name__)

@app.route('/download_ndjson')
def download_ndjson():
    # Path to your existing output.ndjson file
    ndjson_file = 'file.ndjson'
    
    # Send the file
    return send_file(ndjson_file, 
                     mimetype='application/x-ndjson',
                     as_attachment=True,
                     download_name='file.ndjson')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)