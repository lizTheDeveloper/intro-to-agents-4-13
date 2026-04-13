## OODA

## Observe - gather sensor data

### Orient - sythesize sensor data - send the data actually to the language model - assemble the context for the language model

### Decide - <think> ok the user wants me to... </think> ok I'll do that thing

## Act - Do The Thing - tool calls, aka generate json that parses

from openai import OpenAI
import os
client = OpenAI(
    api_key=os.environ.get("GROQ_API_KEY"),
    base_url="https://api.groq.com/openai/v1",
)

response = client.responses.create(
    input="Explain the importance of fast language models",
    model="openai/gpt-oss-20b",
)
print(response.output_text)

