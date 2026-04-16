from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import asyncio

app = FastAPI()


@app.get("/basic", response_class=HTMLResponse)
async def basic():
    return """
    <html>
        <head><title>Basic Page</title></head>
        <body>
            <h1>Hello World</h1>
            <p>This is a basic page for testing.</p>
            <a href="/link1">Link 1</a>
            <a href="/link2">Link 2</a>
        </body>
    </html>
    """


@app.get("/link1", response_class=HTMLResponse)
async def link1():
    return "<html><body><h1>Link 1</h1></body></html>"


@app.get("/link2", response_class=HTMLResponse)
async def link2():
    return "<html><body><h1>Link 2</h1></body></html>"


@app.get("/cookies", response_class=HTMLResponse)
async def cookies():
    return """
    <html>
        <head><title>Cookie Page</title></head>
        <body>
            <h1>Cookie Consent Test</h1>
            <div id="consent-banner">
                <p>We use cookies. Do you accept?</p>
                <button id="accept-button" onclick="document.getElementById('consent-banner').remove()">Accept</button>
            </div>
            <script>
                // simple simulation of a consent manager
                window.setTimeout(() => {
                    console.log("Consent banner loaded");
                }, 100);
            </script>
        </body>
    </html>
    """


@app.get("/slow", response_class=HTMLResponse)
async def slow():
    await asyncio.sleep(2)
    return "<html><body><h1>Slow Page</h1></body></html>"


@app.get("/hang", response_class=HTMLResponse)
async def hang():
    while True:
        await asyncio.sleep(1000)



@app.get("/crash", response_class=HTMLResponse)
async def crash():
    """Page that triggers a browser crash via infinite JS loop."""
    return """
    <html>
        <head><title>Crash Page</title></head>
        <body>
            <script>
                // exhaust memory to force a crash / unresponsive page
                const arrays = [];
                while (true) { arrays.push(new Array(1000000).fill('x')); }
            </script>
        </body>
    </html>
    """
