Render deployment notes

1. Push this folder to a GitHub repo.
2. In Render, create a new Web Service from that repo.
3. Choose Docker runtime, or use Blueprint if you want Render to read render.yaml.
4. No build command is needed because Docker handles it.
5. The app starts with:
   gunicorn --bind 0.0.0.0:$PORT app:app

Local test:

docker build -t v11-recon .
docker run -p 10000:10000 -e PORT=10000 v11-recon

Then open:
http://127.0.0.1:10000
