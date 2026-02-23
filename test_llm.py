from huggingface_hub import InferenceClient
import os
from dotenv import load_dotenv


load_dotenv(".env.local")   
MODEL = "Qwen/Qwen2.5-7B-Instruct"   
client = InferenceClient(
    model=MODEL,
    token=os.getenv("HF_TOKEN")
)

def llm(prompt: str) -> str:
    messages = [{"role": "user", "content": prompt}]

    # Chat-style API
    resp = client.chat.completions.create(
        messages=messages,
        max_tokens=200,
        temperature=0.2,
        top_p=0.9,
    )

    return resp.choices[0].message["content"].strip()

print(llm("Give me 3 high-protein vegetarian lunch ideas under 600 calories."))