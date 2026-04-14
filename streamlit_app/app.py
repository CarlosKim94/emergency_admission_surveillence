import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd
import os

# --- CONFIG & CONNECTION ---
st.set_page_config(layout="wide", page_title="Emergency Surveillance")
if "gcp_service_account" in st.secrets:
    # Use secrets from Streamlit Cloud
    info = json.loads(st.secrets["gcp_service_account"])
    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(credentials=credentials, project=info['project_id'])
else:
    # Fallback for local Docker development
    import os
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/app/keys/emergency-admission-492214-c3756c77628d.json"
    client = bigquery.Client()

@st.cache_data
def get_data():
    # Pull the 'stg_admissions'
    # For this specific dash, we need the raw columns like age_group and relative_cases
    query = """
        SELECT * FROM `emergency-admission-492214.emergency_admission_data.stg_admissions`
    """
    return client.query(query).to_dataframe()

df = get_data()

# --- HEADER SECTION ---
header = st.container()
with header:
    st.title('Emergency Admission for Respiratory Related Diseases')
    st.markdown('<style>div.block-container{padding-top:2rem;}</style>', unsafe_allow_html=True)

    introduction = """
    The data on emergency admissions for respiratory diseases is collected daily as part of the AKTIN registration 
    and published by the Robert Koch Institute (RKI). The dataset includes the date of the admission report, 
    the amount of emergency departments that reported on that day, the average of the admissions per emergency department, 
    the patient's age group and the diagnoses coded according to the 
    [International Classification of Diseases (ICD-10)](https://klassifikationen.bfarm.de/icd-10-gm/kode-suche/htmlgm2026/index.htm).
    The data is processed through the **Airflow + Spark + dbt** pipeline into BigQuery on a weekly basis.
    """
    st.markdown(introduction, unsafe_allow_html=True)
    st.markdown("---")

# --- SIDEBAR FILTERS ---
st.sidebar.header("Filter:")

# Age Group Setup
age_group_order = ['0-4', '5-9', '10-14', '15-19', '20-39', '40-59', '60-79', '80+', '00+']
age_group_options = [age for age in age_group_order if age in df['age_group'].unique()]

syndrome_list = df['syndrome'].unique().tolist()
selected_syndromes = st.sidebar.multiselect("Syndrome", syndrome_list, default=syndrome_list[:3])
selected_ages = st.sidebar.multiselect("Age Group", options=age_group_options)

# --- DATA FILTERING LOGIC ---
filtered_df = df.copy()
if selected_syndromes:
    filtered_df = filtered_df[filtered_df['syndrome'].isin(selected_syndromes)]
if selected_ages:
    filtered_df = filtered_df[filtered_df['age_group'].isin(selected_ages)]

# --- VISUALIZATION 1: LINE CHART ---
# Grouping for the chart
date_syndrome_df = filtered_df.groupby(["admission_date", "syndrome"], as_index=False)["relative_cases"].sum()

fig1 = go.Figure()
config = {'displayModeBar': False}

line_colors = {
    'SARI': 'darksalmon', 
    'ARI': 'steelblue', 
    'ILI': 'seagreen', 
    'GI': 'mediumpurple', 
    'COVID': 'indianred'
}
syndrome_desc = {
    'SARI': 'SARI: Severe Acute Respiratory Infection', 
    'ARI': 'ARI: Acute Respiratory Illness', 
    'ILI': 'ILI: Influenza-Like Illness',
    'GI': 'GI: Gastrointestinal Illness',
    'COVID': 'COVID: COVID-19'
}

for syn in selected_syndromes:
    if syn in date_syndrome_df['syndrome'].unique():
        data = date_syndrome_df[date_syndrome_df['syndrome'] == syn]
        fig1.add_trace(go.Scatter(
            x=data['admission_date'], 
            y=data['relative_cases'],
            mode='lines',
            name=syndrome_desc[syn],
            line=dict(color=line_colors[syn],width=1.5),
            hovertemplate = " %{y:.0f} admissions on %{x}"
        ))

fig1.update_layout(title='Emergency Admissions over time',
                    yaxis_title='Admissions per Emergency Deparment',
                    showlegend=True,
                    legend=dict(x=0.5, y=-0.1, xanchor='center', yanchor='top', orientation='h'),
                    height=600
                    )
st.plotly_chart(fig1, use_container_width=True, config=config)

mid_text = """
For better understanding and identification of peaks over time, the expected value of admissions and an associated 80% prediction interval can be visualized below.
"""
st.markdown(mid_text)

# --- VISUALIZATION 2: Expected upper and lower bounds ---
# Prepare the data for the expected values and prediction intervals
date_syndrome_exp_df = filtered_df.groupby(["admission_date", "syndrome"], as_index=False)[
    ["relative_cases", "expected_lowerbound", "expected_upperbound"]
].sum()

fig_exp = go.Figure()

# Build the chart
for syn in selected_syndromes:
    if syn in date_syndrome_exp_df['syndrome'].unique():
        data = date_syndrome_exp_df[date_syndrome_exp_df['syndrome'] == syn]
        color = line_colors.get(syn, 'gray')

        # Add a line for upper bound
        fig_exp.add_trace(go.Scatter(
            x=data['admission_date'], 
            y=data['expected_upperbound'],
            mode='lines',
            showlegend=False,
            name=syndrome_desc[syn],
            line=dict(color=line_colors[syn], width=0),
            hovertemplate = "Prediction Interval Upperbound: %{y:.0f}"
        ))

        # Add lower bound and shaded fill
        fig_exp.add_trace(go.Scatter(
            x=data['admission_date'], 
            y=data['expected_lowerbound'],
            mode='lines',
            fill='tonexty',  # Fills the area between Trace 1 and Trace 2
            showlegend=False,
            name=syndrome_desc[syn],
            line=dict(color=line_colors[syn], width=0),
            hovertemplate = "Prediction Interval Lowerbound: %{y:.0f}"
        ))

        # Add actual Admission line
        fig_exp.add_trace(go.Scatter(
            x=data['admission_date'], 
            y=data['relative_cases'],
            mode='lines',
            name=syndrome_desc[syn],
            line=dict(color=line_colors[syn], width=1.5),
            hovertemplate = " %{y:.0f} admissions on %{x}"
        ))

# Update Layout
fig_exp.update_layout(title='Emergency Admissions over time with Expected Values',
                    yaxis_title='Admissions per Emergency Deparment',
                    showlegend=True,
                    legend=dict(x=0.5, y=-0.1, xanchor='center', yanchor='top', orientation='h'),
                    height=600
                )

st.plotly_chart(fig_exp, use_container_width=True, config=config)

mid_text_pie = """
    To show which age groups are most affected by different types of respiratory problems, we have created the pie charts below. These charts are arranged from the most to the least serious conditions.
    """
st.markdown(mid_text_pie)

# --- VISUALIZATION 3: PIE CHARTS (AGE GROUPS) ---
st.markdown("###### Syndrome Types by Age Group")

# order age group
age_syndrome_df = filtered_df.groupby(["age_group", "syndrome"], as_index=False)["ed_count"].sum()
age_syndrome_df['age_group'] = pd.Categorical(age_syndrome_df['age_group'], categories=age_group_order, ordered=True)
age_syndrome_df = age_syndrome_df.sort_values("age_group")

# Create Subplots (1 row, N columns)
num_syn = len(selected_syndromes)
fig_pie = make_subplots(
    rows=1, cols=num_syn, 
    specs=[[{'type': 'domain'}] * num_syn], # 'domain' is required for Pie charts
    subplot_titles=selected_syndromes # This puts names right above each chart
)

# Add each Pie trace
for i, syn in enumerate(selected_syndromes):
    syn_data = age_syndrome_df[age_syndrome_df["syndrome"] == syn]

    syn_data = syn_data.set_index('age_group').reindex(age_group_order).reset_index().fillna(0)
    
    fig_pie.add_trace(
        go.Pie(
            labels=syn_data['age_group'],
            values=syn_data['ed_count'],
            sort=False,
            direction='clockwise',
            hole=.3,
            name=syn,
            hovertemplate = (
                "%{percent} of admissions with diagnosis " + syn + "<br>" +
                "are patients in age group %{label}" +
                "<extra></extra>" # 3. This hides the "Trace X" box entirely
            ),
            marker=dict(colors=px.colors.sequential.RdBu),
            # This logic ensures only the FIRST pie chart provides the legend items
            showlegend=(i == 0) 
        ),
        row=1, col=i+1
    )

# 4. Global Layout Adjustments
fig_pie.update_layout(
    height=350,
    margin=dict(t=40, b=120, l=10, r=150),
    legend=dict(
        traceorder="normal",
        orientation="v",
        yanchor="middle", y=0.5,
        xanchor="left", x=1.05 # Pushes legend to the very right of the entire subplot group
    ),
)

# Center the subplot titles and adjust font
fig_pie.update_annotations(font_size=14, y=1.05) 

st.plotly_chart(fig_pie, use_container_width=True, config=config)

# Data source
st.markdown("---")
st.markdown("*Data Source:* https://github.com/robert-koch-institut/Daten_der_Notaufnahmesurveillance")

data_source_desc = """
The data is based on individual willingness to participate in emergency department surveillance reporting as part of the AKTIN emergency room register, and then made available to the RKI. The processing, preparation and automated quality testing as well as the publication of the data are carried out by the [MF 4 | department Specialist and research data management](https://www.rki.de/DE/Content/Institut/OrgEinheiten/MFI/MF4/mf4_node.html).
"""
st.markdown(data_source_desc)