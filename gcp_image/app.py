from flask import Flask, request, jsonify, render_template
import joblib
import numpy as np
import pandas as pd



# # Load preprocessing objects
# scaler = joblib.load('scaler.pkl')
# imputer = joblib.load('imputer.pkl')
# onehot_encoders = joblib.load('onehot_encoders.pkl')

# Load model
with open('best_xgb_model.pkl', 'rb') as file:
    model = joblib.load(file)

app = Flask(__name__)

app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

# def preprocess_new_data(new_example, scaler, imputer, onehot_encoders):
#     # Convert to DataFrame
#     new_df = pd.DataFrame([new_example])
    
#     # Impute missing values using fitted imputer
#     new_df_imputed = pd.DataFrame(
#         imputer.transform(new_df), 
#         columns=new_df.columns
#     )
    
#     # One-hot encode categorical columns
#     for col, encoder in onehot_encoders.items():
#         # Use transform with handle_unknown='ignore'
#         encoded_cols = encoder.transform(new_df_imputed[[col]])
#         encoded_df = pd.DataFrame(
#             encoded_cols, 
#             columns=encoder.get_feature_names_out([col])
#         )
#         new_df_imputed = pd.concat([
#             new_df_imputed.drop(columns=[col]), 
#             encoded_df
#         ], axis=1)
    
#     # Scale numerical columns
#     numerical_cols = new_df_imputed.select_dtypes(include=['int64', 'float64']).columns
#     new_df_imputed[numerical_cols] = scaler.transform(new_df_imputed[numerical_cols])
    
#     return new_df_imputed

@app.route('/')
def home():
    return render_template('index.html')  # Render the HTML form


@app.route('/predict', methods=['POST'])
def predict():
    input_data = request.form.get('input_data')
    print(input_data)
    
    input_list = [[float(x.strip()) for x in input_data.split(',')]]

    input_list=np.array(input_list).reshape(1, -1)
    print(input_list)
    # Preprocess the input
    
    # Make prediction
    prediction = model.predict(input_list)
    return render_template('index.html', prediction=prediction[0])  # Show prediction on the page
    

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)