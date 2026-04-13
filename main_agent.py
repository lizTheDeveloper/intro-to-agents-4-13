## OODA

## Observe - gather sensor data

### Orient - sythesize sensor data - send the data actually to the language model - assemble the context for the language model

### Decide - <think> ok the user wants me to... </think> ok I'll do that thing

## Act - Do The Thing - tool calls, aka generate json that parses

import logging

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s %(message)s",
)

from prompting import prompt
from tools import tool_definitions


print(prompt("Find me interesting CTO or Director of Engineering roles, preferably with Python and JavaScript, preferably in EdTech", tools=tool_definitions))

## Job Searching Agent

##### What does it need in terms of sensor data
### My Resume and Parameters on my Search
### A prompt that explains how to search

##### What tools does it need?
### a web search tool
### A place to store jobs that we've found for further analysis & to apply


##### Prompt


##### Handle Tool Calls

