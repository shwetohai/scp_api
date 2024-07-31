
from fastapi import FastAPI, Form, HTTPException, APIRouter, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from starlette.middleware.base import BaseHTTPMiddleware
import logging
import time
from threading import Thread, Event
import subprocess
import uvicorn
import sys
import requests

# Initialize FastAPI app
app = FastAPI()

# Initialize router
router = APIRouter()

# CORS middleware to allow requests from the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Hardcoded user credentials
# USER_DATA = {
#     "username": "1122334455",
#     "password": "1234"
# }

# OAuth2 token authentication setup
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Middleware for logging requests and responses
class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        logging.info(f"Request: {request.method} {request.url}")
        logging.info(f"Response status: {response.status_code}")
        logging.info(f"Process time: {process_time} seconds")
        return response

app.add_middleware(LoggingMiddleware)

# Event to stop the DICOM SCP server
stop_event = Event()

# Function to start the SCP server
def start_scp_server(token, client_id, branch_id, user_id):
    try:
        # Run the test.py script as an executable
        subprocess.run([sys.executable, "test_concurrency.py", token, client_id, branch_id, user_id])
        #uncomment the below two lines and comment the above line to make the executible file
        #executable_path = "./dist/test_concurrency"
        #executable_path = "./test_concurrency"
        #subprocess.run([executable_path, token, client_id, branch_id, user_id])
    except Exception as e:
        logging.error(f"Failed to start SCP server: {e}")

def call_login_api(mobile, password):
    try:
        login_url = 'https://api.smaro.app/api/auth/client/login'
        headers = {
            'Content-Type': 'application/json'
        }
        data = {
            'mobile': mobile,
            'password': password
        }

        response = requests.post(login_url, headers=headers, json=data, verify=False)
        if response.status_code == 200:
            data = response.json()['data']
            return True, data['token'], data['user']['client_id'], data['user']['client_branch_id'], data['user']['id']
        else:
            return False, "", "", "", ""

    except Exception as e:
        logging.error(f"Error getting login info from login api, Error: {e}")
        return False, "", "", "", ""

# Serve the login HTML page
@app.get("/", response_class=HTMLResponse)
async def read_root():
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Login Page</title>
        <style>
            body {
                display: flex;
                justify-content: center;
                align-items: center;
                height: 100vh;
                background-color: #f0f0f0;
                margin: 0;
            }
            .login-container {
                text-align: center;
                padding: 20px;
                background-color: #fff;
                box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
                border-radius: 8px;
            }
            h2 {
                color: #333;
            }
            form {
                display: flex;
                flex-direction: column;
                align-items: center;
            }
            label {
                margin: 10px 0 5px;
                font-weight: bold;
                color: #555;
            }
            input[type="text"],
            input[type="password"] {
                width: 200px;
                padding: 10px;
                margin-bottom: 15px;
                border: 1px solid #ccc;
                border-radius: 4px;
                font-size: 16px;
            }
            input[type="submit"] {
                padding: 10px 20px;
                border: none;
                border-radius: 4px;
                background-color: #007bff;
                color: white;
                font-size: 16px;
                cursor: pointer;
            }
            input[type="submit"]:hover {
                background-color: #0056b3;
            }
        </style>
        <script>
            window.onload = function() {
                history.pushState(null, document.title, location.href);
                window.addEventListener('popstate', function (event) {
                    history.pushState(null, document.title, location.href);
                });
            };
        </script>
    </head>
    <body>
        <div class="login-container">
            <h2>Login</h2>
            <form action="/login" method="post">
                <label for="mobile">Mobile No:</label>
                <input type="text" id="mobile" name="mobile"><br>
                <label for="password">Password:</label>
                <input type="password" id="password" name="password"><br>
                <input type="submit" value="Login">
            </form>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

# Handle login POST request
@router.post("/login")
async def login(mobile: str = Form(...), password: str = Form(...)):
    flag, token, client_id, branch_id, user_id = call_login_api(mobile, password)
    if flag:
        # Start the SCP server in a separate thread
        global server_thread
        if not stop_event.is_set():
            stop_event.clear()
            server_thread = Thread(target=start_scp_server, args=(
                token, str(client_id), str(branch_id), str(user_id)), daemon=True)
            server_thread.start()
            url = f"https://diagnostics.smaro.app/?mobile={mobile}&password={password}"
            return RedirectResponse(url=url, status_code=302)
        else:
            return JSONResponse(content={"message": "SCP server is already running.", "mobile": mobile})
    else:
        raise HTTPException(status_code=401, detail="Invalid username or password")

# Include the router
app.include_router(router)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1")

# run command - uvicorn app_login:app --port 8009 --reload
# run command - uvicorn app_login:app --port 8009 --reload
#9618057860
'''
pip install pyinstaller

pyinstaller --onefile --hidden-import=pydicom.encoders.gdcm --hidden-import=pydicom.encoders.pylibjpeg test_concurrency.py

pyinstaller --name app_login --onefile app_login.py

./dist/app_login
'''
