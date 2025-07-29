# ML Container Setup Instructions

This container includes popular machine learning libraries and provides a test application to verify everything works correctly.

## Included Libraries

- **NumPy** - Numerical computing
- **Pandas** - Data manipulation and analysis
- **Scikit-learn** - Machine learning algorithms
- **Matplotlib** - Data visualization
- **Seaborn** - Statistical data visualization
- **TensorFlow** - Deep learning framework
- **XGBoost** - Gradient boosting
- **LightGBM** - Gradient boosting
- **Flask** - Web framework for API
- **FastAPI** - Modern API framework
- **Jupyter** - Interactive notebooks

## Quick Start

### Option 1: Using Docker Compose (Recommended)

```bash
# Build and start the container
docker-compose up --build

# Access the application at http://localhost:8000
```

### Option 2: Using Docker directly

```bash
# Build the image
docker build -t ml-container .

# Run the container
docker run -p 8000:8000 ml-container
```

## API Endpoints

Once running, you can test the following endpoints:

- `GET /` - Home page with endpoint information
- `GET /health` - Health check
- `GET /test` - Test all ML libraries
- `POST /predict` - Make predictions with trained model
- `GET /plot` - Generate sample visualization

## Testing the Container

### 1. Basic Health Check
```bash
curl http://localhost:8000/health
```

### 2. Test All Libraries
```bash
curl http://localhost:8000/test
```

### 3. Make a Prediction
```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [1.5, 2.3, 0.8, 1.2]}'
```

### 4. Generate Sample Plot
```bash
curl http://localhost:8000/plot
```

## File Structure

```
.
├── Dockerfile              # Container definition
├── docker-compose.yml      # Multi-container orchestration
├── requirements.txt        # Python dependencies
├── test_app.py             # Main test application
├── README.md               # This file
├── data/                   # Data files (mounted volume)
└── models/                 # Model files (mounted volume)
```

## Customization

### Adding More Libraries

Edit `requirements.txt` to add additional Python packages:

```text
# Add your packages here
tensorflow-hub==0.14.0
transformers==4.21.0
opencv-python==4.8.0.74
```

### Modifying the Test App

Edit `test_app.py` to add your own ML workflows and API endpoints.

### Environment Variables

Set environment variables in `docker-compose.yml`:

```yaml
environment:
  - PYTHONPATH=/app
  - FLASK_ENV=production
  - ML_MODEL_PATH=/app/models
```

## Development Mode

To run in development mode with code hot-reloading:

```bash
# Mount your local code as a volume
docker run -p 8000:8000 -v $(pwd):/app ml-container
```

## Troubleshooting

### Common Issues

1. **Port already in use**: Change the port mapping in docker-compose.yml
2. **Memory issues**: Increase Docker memory allocation for TensorFlow
3. **Permission errors**: Check file permissions in mounted volumes

### Checking Logs

```bash
# View container logs
docker-compose logs -f ml-container

# Or with Docker directly
docker logs -f <container_id>
```

## Production Deployment

For production use:

1. Remove development dependencies from requirements.txt
2. Set `FLASK_ENV=production` 
3. Use a production WSGI server like Gunicorn
4. Add proper error handling and logging
5. Implement authentication if needed

Example production Dockerfile changes:

```dockerfile
# Add Gunicorn
RUN pip install gunicorn

# Change the CMD
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "4", "test_app:app"]
```

