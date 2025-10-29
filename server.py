from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn

from espn_schedule_scraper import compile_season_stats
from database_connector import get_passing_stats_top_n, get_rushing_stats_top_n, get_receiving_stats_top_n, get_defensive_stats_top_n, get_fumbles_stats_top_n, get_interceptions_stats_top_n

app = FastAPI()

# templates directory
templates = Jinja2Templates(directory="templates")


@app.get("/")
async def root() -> RedirectResponse:
    return RedirectResponse(url="/home")

season = 2025
week = 7
season_type = 2 # 1 for preseason, 2 for regular season, 3 for playoffs



@app.get("/home", response_class=HTMLResponse)
async def home(request: Request):
    top_passing_stats = get_passing_stats_top_n(100)
    top_rushing_stats = get_rushing_stats_top_n(300)
    top_receiving_stats = get_receiving_stats_top_n(400)
    top_defensive_stats = get_defensive_stats_top_n(1200)
    top_fumbles_stats = get_fumbles_stats_top_n(300)
    top_interceptions_stats = get_interceptions_stats_top_n(150)
    return templates.TemplateResponse("home.html", {"request": request, "title": "Home", "top_passing_stats": top_passing_stats, "top_rushing_stats": top_rushing_stats, "top_receiving_stats": top_receiving_stats, "top_defensive_stats": top_defensive_stats, "top_fumbles_stats": top_fumbles_stats, "top_interceptions_stats": top_interceptions_stats})

@app.get("/player/{player_id}", response_class=HTMLResponse)
async def player_profile(request: Request, player_id: int):
    # Placeholder implementation for player profile
    
    return templates.TemplateResponse("player.html", {"request": request, "title": "Player Profile", "player_id": player_id})

if __name__ == "__main__":
    #compile_season_stats(season, week, season_type)
    uvicorn.run("server:app", host="127.0.0.1", port=8080, log_level="info", reload=False)
