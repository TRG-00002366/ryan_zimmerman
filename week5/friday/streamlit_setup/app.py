import streamlit as st
import pandas as pd

# Page configuration
st.set_page_config(
    page_title="My First Streamlit App",
    page_icon=":wave:",
    layout="centered"
)

# Title and introduction
st.title("Hello, Streamlit!")
st.write("This is my first Streamlit application.")

# Divider
st.divider()

# Interactive input
st.subheader("Interactive Greeting")
name = st.text_input("What is your name?")
if name:
    st.write(f"Welcome to Streamlit, {name}!")
    st.balloons()

# Divider
st.divider()

# Display some data
st.subheader("Sample Data Display")

# Create sample data
data = {
    "Product": ["Widget A", "Widget B", "Widget C", "Widget D"],
    "Sales": [1200, 1850, 950, 1400],
    "Region": ["North", "South", "East", "West"]
}
df = pd.DataFrame(data)

# Display as table
st.dataframe(df)

# Display metrics
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Sales", f"${df['Sales'].sum():,}")
with col2:
    st.metric("Products", len(df))
with col3:
    st.metric("Avg Sale", f"${df['Sales'].mean():,.0f}")

# Simple chart
st.subheader("Sales by Product")
st.bar_chart(df.set_index("Product")["Sales"])

# Footer
st.divider()
st.caption("Week 6 Visualization - Streamlit Exercise 1")

st.sidebar.title("Settings")
show_data = st.sidebar.checkbox("Show raw data", value=True)
if show_data:
    st.dataframe(df)

selected_region = st.selectbox("Select Region", df["Region"].unique())
filtered_df = df[df["Region"] == selected_region]
st.write(f"Showing data for: {selected_region}")
st.dataframe(filtered_df)

threshold = st.slider("Minimum Sales", 0, 2000, 1000)
above_threshold = df[df["Sales"] >= threshold]
st.write(f"Products above ${threshold}: {len(above_threshold)}")

chart_type = st.radio("Chart Type", ["Bar", "Line"])
if chart_type == "Bar":
    st.bar_chart(df.set_index("Product")["Sales"])
else:
    st.line_chart(df.set_index("Product")["Sales"])