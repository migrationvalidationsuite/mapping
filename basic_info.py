import pandas as pd
import streamlit as st
import numpy as np
from datetime import datetime
import re

class SingleFileDataMapper:
    def __init__(self, excel_file):
        self.excel_file = excel_file
        self.sheets = {}
        self.mapping_config = None
        self.data_sheets = {}
        self.lookup_tables = {}
        
    def load_and_detect_sheets(self):
        """Load Excel file and auto-detect sheet types"""
        try:
            # Read all sheets
            all_sheets = pd.read_excel(self.excel_file, sheet_name=None)
            
            for sheet_name, df in all_sheets.items():
                if sheet_name.upper() in ['MAPPING', 'CONFIG', 'MAPPING_CONFIG']:
                    self.mapping_config = df
                elif sheet_name.upper().startswith('PA00'):
                    self.data_sheets[sheet_name] = df
                elif 'LOOKUP' in sheet_name.upper() or 'REF' in sheet_name.upper():
                    self.lookup_tables[sheet_name] = df
                else:
                    self.sheets[sheet_name] = df
            
            return True
        except Exception as e:
            st.error(f"Error loading Excel file: {str(e)}")
            return False
    
    def get_lookup_value(self, lookup_table, key_col, value_col, key):
        """Get lookup value from reference table"""
        if lookup_table not in self.lookup_tables:
            return key
        
        lookup_df = self.lookup_tables[lookup_table]
        if key_col not in lookup_df.columns or value_col not in lookup_df.columns:
            return key
        
        result = lookup_df[lookup_df[key_col] == key]
        if not result.empty:
            return result.iloc[0][value_col]
        return key
    
    def apply_transformation(self, value, transformation_rule):
        """Apply transformation based on rule"""
        if pd.isna(value) or value == '':
            return None
            
        if not transformation_rule or pd.isna(transformation_rule):
            return value
        
        rule = str(transformation_rule).lower()
        
        # Date transformation
        if 'date' in rule and len(str(value)) == 8:
            try:
                date_str = str(value)
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            except:
                return value
        
        # Gender mapping
        if 'gender' in rule or 'sex' in rule:
            gender_map = {'1': 'Male', 'M': 'Male', 'MALE': 'Male',
                         '2': 'Female', 'F': 'Female', 'FEMALE': 'Female'}
            return gender_map.get(str(value).upper(), value)
        
        # Marital status mapping
        if 'marital' in rule:
            marital_map = {'0': 'Single', '1': 'Married', '2': 'Divorced', '3': 'Widowed'}
            return marital_map.get(str(value), value)
        
        # Name concatenation
        if 'concat' in rule:
            return str(value).strip()
        
        return value
    
    def get_source_value(self, personnel_number, source_table, source_field, subtype=None):
        """Get value from source table for specific personnel number"""
        if source_table not in self.data_sheets:
            return None
            
        df = self.data_sheets[source_table]
        
        # Handle different personnel number column names
        pernr_cols = ['PERNR', 'Personnel Number', 'PersonnelNumber', 'EmpID', 'EmployeeID']
        pernr_col = None
        for col in pernr_cols:
            if col in df.columns:
                pernr_col = col
                break
        
        if pernr_col is None:
            return None
        
        # Filter by personnel number
        emp_data = df[df[pernr_col] == personnel_number]
        
        # Filter by subtype if specified
        if subtype and 'SUBTY' in df.columns:
            emp_data = emp_data[emp_data['SUBTY'] == subtype]
        elif subtype and 'Subtype' in df.columns:
            emp_data = emp_data[emp_data['Subtype'] == subtype]
        
        if emp_data.empty:
            return None
        
        # Handle different source field variations
        possible_fields = [source_field, source_field.upper(), source_field.lower()]
        for field in possible_fields:
            if field in emp_data.columns:
                value = emp_data.iloc[0][field]
                return value if not pd.isna(value) else None
        
        return None
    
    def transform_data(self):
        """Transform data according to mapping configuration"""
        if self.mapping_config is None:
            st.error("No mapping configuration found")
            return None
        
        # Get unique personnel numbers from PA0002 (personal data)
        pa0002_data = self.data_sheets.get('PA0002')
        if pa0002_data is None:
            st.error("PA0002 sheet not found")
            return None
        
        # Find personnel number column
        pernr_cols = ['PERNR', 'Personnel Number', 'PersonnelNumber', 'EmpID', 'EmployeeID']
        pernr_col = None
        for col in pernr_cols:
            if col in pa0002_data.columns:
                pernr_col = col
                break
        
        if pernr_col is None:
            st.error("Personnel number column not found in PA0002")
            return None
        
        personnel_numbers = pa0002_data[pernr_col].unique()
        
        # Initialize result dataframe
        target_fields = self.mapping_config['Target Field'].tolist()
        result_data = []
        
        for pernr in personnel_numbers:
            if pd.isna(pernr):
                continue
                
            row_data = {}
            
            for _, mapping_row in self.mapping_config.iterrows():
                target_field = mapping_row.get('Target Field')
                source_table = mapping_row.get('Source Table')
                source_field = mapping_row.get('Source Field')
                transformation = mapping_row.get('Transformation')
                notes = mapping_row.get('Notes', '')
                
                if pd.isna(target_field):
                    continue
                
                # Extract subtype from notes if present
                subtype = None
                if isinstance(notes, str) and 'subty' in notes.lower():
                    subtype_match = re.search(r'subty[=:\s]*(\d+)', notes.lower())
                    if subtype_match:
                        subtype = subtype_match.group(1)
                
                # Get source value
                if pd.isna(source_table) or pd.isna(source_field):
                    value = None
                else:
                    value = self.get_source_value(pernr, source_table, source_field, subtype)
                
                # Apply transformation
                if value is not None:
                    value = self.apply_transformation(value, transformation)
                
                row_data[target_field] = value
            
            result_data.append(row_data)
        
        result_df = pd.DataFrame(result_data)
        
        # Handle special cases for name concatenation
        if 'displayName' in result_df.columns:
            if 'firstName' in result_df.columns and 'lastName' in result_df.columns:
                result_df['displayName'] = (
                    result_df['firstName'].astype(str) + ' ' + result_df['lastName'].astype(str)
                ).replace('nan nan', None)
        
        # Replace NaN with None for cleaner display
        result_df = result_df.where(pd.notnull(result_df), None)
        
        return result_df
    
    def get_data_quality_report(self, transformed_data):
        """Generate data quality report"""
        if transformed_data is None:
            return {}
        
        total_records = len(transformed_data)
        report = {
            'total_records': total_records,
            'field_completeness': {}
        }
        
        for col in transformed_data.columns:
            non_null_count = transformed_data[col].count()
            completeness = (non_null_count / total_records) * 100 if total_records > 0 else 0
            report['field_completeness'][col] = {
                'non_null_count': non_null_count,
                'completeness_percent': round(completeness, 2)
            }
        
        return report

# Streamlit Interface
def main():
    st.title("HR Data Mapping Tool - Enhanced Version")
    st.write("Transform SAP HR data to SuccessFactors format")
    
    # File upload
    uploaded_file = st.file_uploader("Upload Excel file with mapping config and data", type=['xlsx', 'xls'])
    
    if uploaded_file is not None:
        # Initialize mapper
        mapper = SingleFileDataMapper(uploaded_file)
        
        # Load and detect sheets
        if mapper.load_and_detect_sheets():
            st.success("✅ File loaded successfully!")
            
            # Show detected sheets
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.write("**Mapping Config:**")
                st.write("✅ Found" if mapper.mapping_config is not None else "❌ Missing")
            
            with col2:
                st.write("**Data Sheets:**")
                for sheet_name in mapper.data_sheets.keys():
                    st.write(f"✅ {sheet_name}")
            
            with col3:
                st.write("**Lookup Tables:**")
                for lookup_name in mapper.lookup_tables.keys():
                    st.write(f"✅ {lookup_name}")
            
            # Show mapping configuration preview
            if mapper.mapping_config is not None:
                st.subheader("Mapping Configuration Preview")
                st.dataframe(mapper.mapping_config.head(10))
            
            # Show data preview
            if mapper.data_sheets:
                st.subheader("Source Data Preview")
                selected_sheet = st.selectbox("Select sheet to preview:", list(mapper.data_sheets.keys()))
                if selected_sheet:
                    st.dataframe(mapper.data_sheets[selected_sheet].head(5))
            
            # Transform button
            if st.button("🔄 Transform Data", type="primary"):
                with st.spinner("Transforming data..."):
                    transformed_data = mapper.transform_data()
                    
                    if transformed_data is not None:
                        st.success("✅ Data transformed successfully!")
                        
                        # Show results
                        st.subheader("Transformed Data")
                        st.dataframe(transformed_data)
                        
                        # Data quality report
                        quality_report = mapper.get_data_quality_report(transformed_data)
                        
                        st.subheader("Data Quality Report")
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.metric("Total Records", quality_report['total_records'])
                        
                        with col2:
                            avg_completeness = np.mean([field['completeness_percent'] 
                                                      for field in quality_report['field_completeness'].values()])
                            st.metric("Average Completeness", f"{avg_completeness:.1f}%")
                        
                        # Field completeness details
                        completeness_data = []
                        for field, stats in quality_report['field_completeness'].items():
                            completeness_data.append({
                                'Field': field,
                                'Records with Data': stats['non_null_count'],
                                'Completeness %': stats['completeness_percent']
                            })
                        
                        completeness_df = pd.DataFrame(completeness_data)
                        st.dataframe(completeness_df.sort_values('Completeness %', ascending=False))
                        
                        # Download options
                        st.subheader("Download Results")
                        
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            excel_buffer = pd.ExcelWriter('transformed_data.xlsx', engine='xlsxwriter')
                            transformed_data.to_excel(excel_buffer, index=False, sheet_name='Transformed_Data')
                            excel_buffer.close()
                            
                            st.download_button(
                                label="📊 Download Excel",
                                data=excel_buffer,
                                file_name="hr_transformed_data.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        
                        with col2:
                            csv_data = transformed_data.to_csv(index=False)
                            st.download_button(
                                label="📝 Download CSV",
                                data=csv_data,
                                file_name="hr_transformed_data.csv",
                                mime="text/csv"
                            )
                        
                        with col3:
                            json_data = transformed_data.to_json(orient='records', indent=2)
                            st.download_button(
                                label="🔗 Download JSON",
                                data=json_data,
                                file_name="hr_transformed_data.json",
                                mime="application/json"
                            )

if __name__ == "__main__":
    main()
