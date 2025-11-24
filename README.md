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
