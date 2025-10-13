import os
import json
from flask import Flask, request, jsonify
import zstandard as zstd
import gcsfs

app = Flask(__name__)

def stream_decompress_and_write(input_path, output_path):
    """
    decompresses chunks and write them directly to the GCS file.
    """
    fs = gcsfs.GCSFileSystem()
    dctx = zstd.ZstdDecompressor(max_window_size=2**31)
    
    lines_processed = 0
    buffer = ""
    try:
        with fs.open(input_path, "rb") as input_fh:
            with dctx.stream_reader(input_fh) as reader:
                # File for writing stream to GCS
                with fs.open(output_path, "w") as output_fh:
                    while True:
                        # 128 MB chunk (zst stream)
                        chunk = reader.read(2**27)
                        if not chunk:
                            break
                        chunk_str = buffer + chunk.decode("utf-8", errors="ignore")
                        lines = chunk_str.split("\n")
                        
                        buffer = lines[-1]
                        for line in lines[:-1]:
                            if line.strip():
                                try:
                                    data = json.loads(line)
                                    output_fh.write(json.dumps(data) + "\n")
                                    lines_processed += 1
                                except json.JSONDecodeError:
                                    continue
                    # final line
                    if buffer.strip():
                         try:
                             data = json.loads(buffer)
                             output_fh.write(json.dumps(data) + "\n")
                             lines_processed += 1
                         except json.JSONDecodeError:
                             # ignore incomplete last line
                             pass 
    except Exception as e:
        print(f"[{input_path}] Processing error: {e}")
        raise e
        
    return lines_processed

@app.route("/decompress", methods=["POST"])
def decompress():
    data = request.get_json()
    bucket_name = data.get("bucket_name")
    input_file = data.get("input_file")
    output_file = data.get("output_file")

    if not bucket_name or not input_file or not output_file:
        return jsonify({"error": "bucket_name, input_file, and output_file are required"}), 400

    input_path = f"gs://{bucket_name}/raw/{input_file}"
    output_path = f"gs://{bucket_name}/decompressed/{output_file}"

    try:
        count = stream_decompress_and_write(input_path, output_path)
    except Exception as e:
         return jsonify({"error": f"Failed to process file: {str(e)}"}), 500

    return jsonify({"message": f"Wrote decompressed results to {output_path}", "count": count}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
