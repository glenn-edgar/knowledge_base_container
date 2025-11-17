#!/usr/bin/env python3
"""
ML Container Test Application
Tests various machine learning libraries and provides a simple API
"""

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import make_classification
from sklearn.metrics import accuracy_score, classification_report
import matplotlib.pyplot as plt
import seaborn as sns
from flask import Flask, jsonify, request
import io
import base64
import json

app = Flask(__name__)

class MLTester:
    def __init__(self):
        self.model = None
        self.X_test = None
        self.y_test = None
        self.predictions = None
        
    def test_libraries(self):
        """Test that all ML libraries are working correctly"""
        results = {}
        
        try:
            # Test NumPy
            arr = np.array([1, 2, 3, 4, 5])
            results['numpy'] = {
                'status': 'OK',
                'version': np.__version__,
                'test': f"Array sum: {np.sum(arr)}"
            }
        except Exception as e:
            results['numpy'] = {'status': 'ERROR', 'error': str(e)}
            
        try:
            # Test Pandas
            df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
            results['pandas'] = {
                'status': 'OK',
                'version': pd.__version__,
                'test': f"DataFrame shape: {df.shape}"
            }
        except Exception as e:
            results['pandas'] = {'status': 'ERROR', 'error': str(e)}
            
        try:
            # Test Scikit-learn
            from sklearn import __version__ as sklearn_version
            X, y = make_classification(n_samples=100, n_features=4, random_state=42)
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            clf = RandomForestClassifier(random_state=42)
            clf.fit(X_train, y_train)
            predictions = clf.predict(X_test)
            accuracy = accuracy_score(y_test, predictions)
            
            results['scikit-learn'] = {
                'status': 'OK',
                'version': sklearn_version,
                'test': f"Classification accuracy: {accuracy:.3f}"
            }
            
            # Store for later use
            self.model = clf
            self.X_test = X_test
            self.y_test = y_test
            self.predictions = predictions
            
        except Exception as e:
            results['scikit-learn'] = {'status': 'ERROR', 'error': str(e)}
            
        try:
            # Test Matplotlib
            import matplotlib
            results['matplotlib'] = {
                'status': 'OK',
                'version': matplotlib.__version__,
                'test': "Plot generation capability available"
            }
        except Exception as e:
            results['matplotlib'] = {'status': 'ERROR', 'error': str(e)}
            
        try:
            # Test Seaborn
            results['seaborn'] = {
                'status': 'OK',
                'version': sns.__version__,
                'test': "Statistical visualization capability available"
            }
        except Exception as e:
            results['seaborn'] = {'status': 'ERROR', 'error': str(e)}
            
        return results
    
    def generate_sample_plot(self):
        """Generate a sample plot and return as base64 string"""
        try:
            if self.model is None:
                return None
                
            # Create a confusion matrix plot
            from sklearn.metrics import confusion_matrix
            import matplotlib.pyplot as plt
            
            cm = confusion_matrix(self.y_test, self.predictions)
            
            plt.figure(figsize=(8, 6))
            sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
            plt.title('Confusion Matrix')
            plt.ylabel('Actual')
            plt.xlabel('Predicted')
            
            # Save plot to base64 string
            img_buffer = io.BytesIO()
            plt.savefig(img_buffer, format='png')
            img_buffer.seek(0)
            img_base64 = base64.b64encode(img_buffer.getvalue()).decode()
            plt.close()
            
            return img_base64
        except Exception as e:
            return f"Error generating plot: {str(e)}"

# Initialize ML Tester
ml_tester = MLTester()

@app.route('/')
def home():
    """Home endpoint with basic info"""
    return jsonify({
        'message': 'ML Container Test Application',
        'endpoints': {
            '/': 'This endpoint',
            '/test': 'Test all ML libraries',
            '/health': 'Health check',
            '/predict': 'Make predictions (POST)',
            '/plot': 'Generate sample plot (JSON with base64)',
            '/plot/download': 'Download plot as PNG file',
            '/plot/html': 'View plot directly in browser'
        }
    })

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'message': 'ML container is running'})

@app.route('/test')
def test_libraries():
    """Test all ML libraries endpoint"""
    results = ml_tester.test_libraries()
    return jsonify(results)

@app.route('/predict', methods=['POST'])
def predict():
    """Make predictions using the trained model"""
    try:
        if ml_tester.model is None:
            # Train a model first
            ml_tester.test_libraries()
        
        # Get data from request
        data = request.get_json()
        if not data or 'features' not in data:
            return jsonify({'error': 'Please provide features array'}), 400
            
        features = np.array(data['features']).reshape(1, -1)
        prediction = ml_tester.model.predict(features)
        probability = ml_tester.model.predict_proba(features)
        
        return jsonify({
            'prediction': int(prediction[0]),
            'probability': {
                'class_0': float(probability[0][0]),
                'class_1': float(probability[0][1])
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/plot')
def get_plot():
    """Generate and return a sample plot"""
    if ml_tester.model is None:
        ml_tester.test_libraries()
    
    plot_data = ml_tester.generate_sample_plot()
    if plot_data:
        return jsonify({
            'plot': plot_data,
            'format': 'base64_png',
            'message': 'Confusion matrix for sample classification',
            'instructions': {
                'view_in_browser': 'Copy the base64 string and paste into: data:image/png;base64,<base64_string>',
                'save_to_file': 'Use the /plot/download endpoint to get the image directly'
            }
        })
    else:
        return jsonify({'error': 'Could not generate plot'}), 500

@app.route('/plot/download')
def download_plot():
    """Generate and return a plot as a downloadable PNG file"""
    if ml_tester.model is None:
        ml_tester.test_libraries()
    
    plot_data = ml_tester.generate_sample_plot()
    if plot_data and isinstance(plot_data, str):
        try:
            # Decode base64 to binary
            import base64
            from flask import Response
            
            img_binary = base64.b64decode(plot_data)
            
            return Response(
                img_binary,
                mimetype='image/png',
                headers={
                    'Content-Disposition': 'attachment; filename=confusion_matrix.png',
                    'Content-Type': 'image/png'
                }
            )
        except Exception as e:
            return jsonify({'error': f'Could not process plot: {str(e)}'}), 500
    else:
        return jsonify({'error': 'Could not generate plot'}), 500

@app.route('/plot/html')
def get_plot_html():
    """Generate and return a plot embedded in HTML for direct viewing"""
    if ml_tester.model is None:
        ml_tester.test_libraries()
    
    plot_data = ml_tester.generate_sample_plot()
    if plot_data and isinstance(plot_data, str):
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>ML Container - Confusion Matrix</title>
            <style>
                body {{ 
                    font-family: Arial, sans-serif; 
                    text-align: center; 
                    margin: 20px;
                    background-color: #f5f5f5;
                }}
                .container {{
                    background-color: white;
                    padding: 20px;
                    border-radius: 10px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                    display: inline-block;
                }}
                h1 {{ color: #333; }}
                img {{ 
                    max-width: 100%; 
                    height: auto; 
                    border: 1px solid #ddd;
                    border-radius: 5px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🧠 ML Container - Confusion Matrix</h1>
                <p>Sample classification results visualization</p>
                <img src="data:image/png;base64,{plot_data}" alt="Confusion Matrix" />
                <p><small>Generated by the ML Container test application</small></p>
            </div>
        </body>
        </html>
        """
        from flask import Response
        return Response(html_content, mimetype='text/html')
    else:
        return jsonify({'error': 'Could not generate plot'}), 500

if __name__ == '__main__':
    print("🚀 Starting ML Container Test Application...")
    print("📊 Testing ML libraries...")
    
    # Run initial tests
    test_results = ml_tester.test_libraries()
    print("\n📋 Library Test Results:")
    for lib, result in test_results.items():
        status = result['status']
        if status == 'OK':
            print(f"  ✅ {lib}: {result.get('version', 'N/A')} - {result.get('test', '')}")
        else:
            print(f"  ❌ {lib}: {result.get('error', 'Unknown error')}")
    
    print(f"\n🌐 Starting Flask server...")
    print(f"🔗 Access the API at: http://localhost:8000")
    print(f"📖 Available endpoints:")
    print(f"  • GET  / - Home page")
    print(f"  • GET  /test - Test all libraries")
    print(f"  • GET  /health - Health check")
    print(f"  • POST /predict - Make predictions")
    print(f"  • GET  /plot - Generate sample plot (JSON)")
    print(f"  • GET  /plot/download - Download plot as PNG")
    print(f"  • GET  /plot/html - View plot in browser")
    
    app.run(host='0.0.0.0', port=8000, debug=True)