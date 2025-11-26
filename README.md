# price-list
Price List Update

## Login (Admin)

This project includes a minimal admin login mechanism. Set the admin password using the `ADMIN_PASSWORD` environment variable before running the app. Example (Linux):

```
export ADMIN_PASSWORD="sua-senha-admin"
export SECRET_KEY="uma-chave-secreta"  # opcional, para assinar cookies
uvicorn app:app --reload
```

Visit `/login` in your browser to sign in. The server sets a signed `session` cookie for authenticated admin access. Use `/logout` to clear the session cookie.

Note: This is a lightweight, self-contained solution intended for internal tools. For production use, consider TLS (HTTPS), stronger session management and secure cookie flags.

## Docker

You can run the app in Docker. The `lists` folder on the host will be mounted into the container so you can inspect generated files easily.

Using docker-compose (recommended):

```
# set required env vars
export ADMIN_PASSWORD="your-admin-password"
export SECRET_KEY="a-secret-key"

# build and start
docker compose up --build
```

This maps:
- `./lists` -> `/app/lists` (accessible inside the container)
- `./prepare-lists/output` -> `/app/prepare-lists/output`

Or run without compose:

```
docker build -t price-list .
docker run -p 8000:8000 -e ADMIN_PASSWORD="your-admin-password" -e SECRET_KEY="a-secret" -v "$(pwd)/lists:/app/lists" -v "$(pwd)/prepare-lists/output:/app/prepare-lists/output" price-list
```

Visit `http://localhost:8000/login` after the container starts.

Tip: instead of exporting variables manually, copy `.env.example` to `.env` and edit values there. `docker compose` will pick up `.env` when `env_file: .env` is configured in `docker-compose.yml`.
