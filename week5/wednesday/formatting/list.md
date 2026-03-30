Performance Status = 
SWITCH(
    TRUE(),
    [YoY Growth] >= 0.10, "Exceeding",
    [YoY Growth] >= 0, "Meeting",
    "Below Target"
)

Growth Color = 
SWITCH(
    TRUE(),
    [YoY Growth] >= 0.10, "#2E7D32",  // Dark green
    [YoY Growth] >= 0, "#FFC107",     // Yellow
    "#D32F2F"                          // Red
)