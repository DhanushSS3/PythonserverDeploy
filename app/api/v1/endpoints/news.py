from fastapi import APIRouter, Depends, HTTPException, status, Response
import httpx
from app.core.security import get_current_user
from app.database.models import User, DemoUser

router = APIRouter(
    prefix="/news",
    tags=["news"]
)

@router.get(
    "",
    summary="Get latest news",
    description="Fetches the latest news from the RSS feed."
)
async def get_news(
    current_user: User | DemoUser = Depends(get_current_user)
):
    """
    Fetches the latest news from the RSS feed.
    Requires authentication with JWT.
    Returns the XML response directly.
    """
    url = "https://feeds.feedburner.com/Newsmovesmarketsforex/khSh"
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            
        if response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Failed to fetch news from the external service"
            )
            
        # Return the XML content directly
        return Response(
            content=response.content,
            media_type="application/xml"
        )
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching news: {str(e)}"
        ) 