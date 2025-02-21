from fastapi.security import OAuth2PasswordBearer

# Use a consistent token endpoint name. In this example, we use "/token".
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
