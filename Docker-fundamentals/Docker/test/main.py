import pandas as pd

blocks = pd.DataFrame({
    "block_number": [101, 102, 103],
    "transactions": [145, 231, 198]
})

print(blocks)

# Simple crypto metric: average transactions per block
print("Average transactions:", blocks["transactions"].mean())