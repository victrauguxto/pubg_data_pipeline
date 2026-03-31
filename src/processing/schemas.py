# src/processing/schemas.py
"""Define data schemas (e.g., using pydantic or dataclasses).
Placeholder – add real schema definitions later.
"""

# Example placeholder using dataclasses
from dataclasses import dataclass

@dataclass
class Match:
    id: str
    season_id: str
    # add more fields as needed

@dataclass
class Player:
    id: str
    name: str
    # add more fields as needed
