"""兼容旧入口：实际实现在 app 包内。

直接 `python main.py` 或 `uvicorn main:app` 的行为不变。
"""
from app.main import app, main

if __name__ == "__main__":
    main()
