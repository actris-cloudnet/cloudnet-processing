#!/usr/bin/env python3
import uvicorn

if __name__ == "__main__":
    uvicorn.run("data_processing.data_submission_api:app",
                host='0.0.0.0',
                port=int(5700),
                reload=True,
                debug=True,
                workers=1)
