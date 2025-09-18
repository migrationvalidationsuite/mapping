import pandas as pd
import streamlit as st
import numpy as np
from datetime import datetime
import re
from io import BytesIO

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
                clean_name = sheet_name.strip()
                
                # Check if this is a mapping configuration sheet
                if self._is_mapping_sheet(df):
                    self.mapping_config = df
                    st.success(f"Found mapping configuration in sheet: {clean_name}")
                # Check if this is a data sheet
                elif self._is_data_sheet(df, clean_name):
                    self.data_sheets[clean_name] = df
                    st.success(f"Found data sheet: {clean_name}")
                elif 'LOOKUP' in clean_name.upper() or 'REF' in clean_name.upper():
                    self.lookup_tables[clean_name] = df
                else:
                    self.sheets[clean_name] = df
            
            return True
        except Exception as e:
            st.error(f"Error loading Excel file: {str(e)}")
            return False
    
    def _is_mapping_sheet(self, df):
        """Check if dataframe is a mapping configuration sheet"""
        columns = [col.lower() for col in df.columns]
        # Look for key mapping columns
        has_target = any('target column' in col for col in columns)
        has_source = any('source' in col and ('table' in col or 'field' in col) for col in columns)
        return has_target and has_source
    
    def _is_data_sheet(self, df, sheet_name):
        """Check if dataframe is a data sheet"""
        # Check for PERNR column (personnel number)
        has_pernr = 'PERNR' in df.columns
        # Check for PA sheet naming convention
        is_pa_sheet = sheet_name.upper().startswith('PA0')
        return has_pernr or is_pa_sheet
    
    def get_source_value(self, personnel_number, source_table, source_field, subtype=None, notes=""):
        """Get value from source table for specific personnel number"""
        # Find the matching data sheet
        target_sheet = None
        for sheet_name, df in self.data_sheets.items():
            if source_table in sheet_name.upper() or sheet_name.upper() in source_table.upper():
                target_sheet = df
                break
        
        if target_sheet is None:
            return None
        
        # Handle different personnel number column names
        pernr_col = None
        for col in ['PERNR', 'Personnel Number', 'PersonnelNumber', 'EmpID', 'EmployeeID']:
            if col in target_sheet.columns:
                pernr_col = col
                break
        
        if pernr_col is None:
            return None
        
        # Convert personnel number to string for comparison
        target_sheet = target_sheet.copy()
        target_sheet[pernr_col] = target_sheet[pernr_col].astype(str)
        personnel_number = str(personnel_number)
        
        # Filter by personnel number
        emp_data = target_sheet[target_sheet[pernr_col] == personnel_number]
        
        if emp_data.empty:
            return None
        
        # Handle subtype filtering for communication and address data
        if subtype or 'SUBTY' in notes:
            subtype_value = self._extract_subtype_from_notes(notes) if not subtype else subtype
            if subtype_value and 'SUBTY' in emp_data.columns:
                emp_data = emp_data[emp_data['SUBTY'] == int(subtype_value)]
                if emp_data.empty:
                    return None
        
        # Get the field value
        if source_field in emp_data.columns:
            value = emp_data.iloc[0][source_field]
            return value if not pd.isna(value) else None
        
        return None
    
    def _extract_subtype_from_notes(self, notes):
        """Extract subtype number from notes field"""
        if not isinstance(notes, str):
            return None
        
        # Look for patterns like SUBTY=0010, SUBTY=10, etc.
        match = re.search(r'SUBTY[=:\s]*(\d+)', notes.upper())
        if match:
            return match.group(1).lstrip('0') or '0'  # Remove leading zeros but keep single 0
        
        # Look for specific mentions
        if 'EMAIL' in notes.upper():
            return '10'  # Email subtype
        elif 'PHONE' in notes.upper():
            return '20'  # Phone subtype
        
        return None
    
    def apply_transformation(self, value, transformation_rule, field_name="", person_row=None):
        """Apply transformation based on rule and field context"""
        if pd.isna(value) or value == '':
            return None
            
        if not transformation_rule or pd.isna(transformation_rule):
            # Apply default transformations based on field name
            return self._apply_default_transformations(value, field_name, person_row)
        
        rule = str(transformation_rule)
        
        # Date transformation
        if 'date' in rule.lower() or len(str(value)) == 8:
            try:
                date_str = str(value)
                if len(date_str) == 8 and date_str.isdigit():
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            except:
                pass
        
        # Concatenation for display name
        if 'concatenate' in rule.lower() and person_row is not None:
            if 'VORNA' in rule and 'NACHN' in rule:
                first_name = person_row.get('VORNA', '') or ''
                last_name = person_row.get('NACHN', '') or ''
                return f"{first_name} {last_name}".strip()
        
        # Apply default transformations
        return self._apply_default_transformations(value, field_name, person_row)
    
    def _apply_default_transformations(self, value, field_name="", person_row=None):
        """Apply default transformations based on field name patterns"""
        field_name = field_name.upper()
        
        # Gender transformation
        if 'GESCH' in field_name or 'GENDER' in field_name.upper():
            gender_map = {'1': 'Male', 'M': 'Male', 'MALE': 'Male',
                         '2': 'Female', 'F': 'Female', 'FEMALE': 'Female'}
            return gender_map.get(str(value).upper(), value)
        
        # Marital status transformation
        if 'FAMST' in field_name or 'MARITAL' in field_name.upper():
            marital_map = {'0': 'Single', '1': 'Married', '2': 'Divorced', 
                          '3': 'Widowed', '4': 'Separated'}
            return marital_map.get(str(value), value)
        
        # Date fields
        if any(date_field in field_name for date_field in ['GBDAT', 'BEGDA', 'ENDDA', 'DATE']):
            if len(str(value)) == 8 and str(value).isdigit():
                try:
                    date_str = str(value)
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                except:
                    pass
        
        return value
    
    def transform_data(self):
        """Transform data according to mapping configuration"""
        if self.mapping_config is None:
            st.error("No mapping configuration found")
            return None
        
        # Get PA0002 data (personal data)
        pa0002_data = None
        for sheet_name, df in self.data_sheets.items():
            if 'PA0002' in sheet_name.upper() or 'PERSONAL' in sheet_name.upper():
                pa0002_data = df
                break
        
        if pa0002_data is None:
            st.error("PA0002 (Personal Data) sheet not found")
            return None
        
        # Find personnel number column
        pernr_col = None
        for col in ['PERNR', 'Personnel Number', 'PersonnelNumber', 'EmpID']:
            if col in pa0002_data.columns:
                pernr_col = col
                break
        
        if pernr_col is None:
            st.error("Personnel number column not found in PA0002")
            return None
        
        # Get unique personnel numbers
        personnel_numbers = pa0002_data[pernr_col].dropna().unique()
        st.info(f"Processing {len(personnel_numbers)} employees...")
        
        # Find column names in mapping config
        target_col = None
        source_table_col = None
        source_field_col = None
        notes_col = None
        
        for col in self.mapping_config.columns:
            col_lower = col.lower()
            if 'target column' in col_lower and 'successfactor' in col_lower:
                target_col = col
            elif 'source table' in col_lower:
                source_table_col = col
            elif 'technical field' in col_lower or ('source field' in col_lower and 'ecc' in col_lower):
                source_field_col = col
            elif 'notes' in col_lower or 'transformation' in col_lower:
                notes_col = col
        
        if not all([target_col, source_table_col, source_field_col]):
            st.error("Could not find required columns in mapping configuration")
            return None
        
        # Process each employee
        result_data = []
        
        progress_bar = st.progress(0)
        
        for idx, pernr in enumerate(personnel_numbers):
            if pd.isna(pernr):
                continue
            
            row_data = {}
            
            # Get person's PA0002 record for transformations
            person_pa0002 = pa0002_data[pa0002_data[pernr_col] == pernr]
            person_row = person_pa0002.iloc[0] if not person_pa0002.empty else None
            
            # Process each mapping rule
            for _, mapping_row in self.mapping_config.iterrows():
                target_field = mapping_row.get(target_col)
                source_table = mapping_row.get(source_table_col)
                source_field = mapping_row.get(source_field_col)
                notes = mapping_row.get(notes_col, '') if notes_col else ''
                
                if pd.isna(target_field) or target_field.strip() == '':
                    continue
                
                # Get source value
                if pd.isna(source_table) or pd.isna(source_field):
                    value = None
                else:
                    value = self.get_source_value(pernr, source_table, source_field, notes=notes)
                
                # Apply transformations
                if value is not None:
                    value = self.apply_transformation(value, notes, source_field, person_row)
                
                row_data[target_field] = value
            
            result_data.append(row_data)
            
            # Update progress
            progress_bar.progress((idx + 1) / len(personnel_numbers))
        
        result_df = pd.DataFrame(result_data)
        
        # Replace NaN with None for cleaner display
        result_df = result_df.where(pd.notnull(result_df), None)
        
        st.success(f"✅ Successfully transformed {len(result_df)} employee records!")
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
    st.set_page_config(
        page_title="HR Data Mapping Tool - Fixed",
        page_icon="🔄",
        layout="wide"
    )
    
    st.title("🔄 HR Data Mapping Tool - Fixed Version")
    st.write("Transform SAP HR data to SuccessFactors format")
    
    # File upload
    uploaded_file = st.file_uploader(
        "Upload Excel file with mapping config and data", 
        type=['xlsx', 'xls'],
        help="File should contain mapping configuration and PA0002, PA0105, PA0006 data sheets"
    )
    
    if uploaded_file is not None:
        # Initialize mapper
        mapper = SingleFileDataMapper(uploaded_file)
        
        # Load and detect sheets
        st.subheader("Loading File...")
        if mapper.load_and_detect_sheets():
            
            # Show detected sheets summary
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Mapping Config", "✅ Found" if mapper.mapping_config is not None else "❌ Missing")
            
            with col2:
                st.metric("Data Sheets", len(mapper.data_sheets))
                for sheet_name in mapper.data_sheets.keys():
                    st.write(f"• {sheet_name}")
            
            with col3:
                st.metric("Lookup Tables", len(mapper.lookup_tables))
                for lookup_name in mapper.lookup_tables.keys():
                    st.write(f"• {lookup_name}")
            
            # Show mapping configuration preview
            if mapper.mapping_config is not None:
                st.subheader("📋 Mapping Configuration Preview")
                st.dataframe(mapper.mapping_config.head(10), use_container_width=True)
            
            # Show source data previews
            if mapper.data_sheets:
                st.subheader("📊 Source Data Preview")
                
                tabs = st.tabs(list(mapper.data_sheets.keys()))
                for i, (sheet_name, df) in enumerate(mapper.data_sheets.items()):
                    with tabs[i]:
                        st.write(f"**{sheet_name}** - {len(df)} rows, {len(df.columns)} columns")
                        
                        # Show unique employee count if PERNR exists
                        if 'PERNR' in df.columns:
                            unique_count = df['PERNR'].nunique()
                            st.write(f"Unique employees: {unique_count}")
                        
                        st.dataframe(df.head(5), use_container_width=True)
            
            # Transform button
            st.subheader("🚀 Transform Data")
            
            if st.button("Transform Data", type="primary", use_container_width=True):
                with st.spinner("Transforming data... This may take a few minutes for large datasets."):
                    transformed_data = mapper.transform_data()
                    
                    if transformed_data is not None:
                        st.success("🎉 Data transformation completed!")
                        
                        # Show results
                        st.subheader("📋 Transformed Data")
                        st.dataframe(transformed_data, use_container_width=True)
                        
                        # Data quality report
                        quality_report = mapper.get_data_quality_report(transformed_data)
                        
                        st.subheader("📊 Data Quality Report")
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
                        st.dataframe(
                            completeness_df.sort_values('Completeness %', ascending=False),
                            use_container_width=True
                        )
                        
                        # Download options
                        st.subheader("💾 Download Results")
                        
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            # Excel download
                            buffer = BytesIO()
                            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                                transformed_data.to_excel(writer, sheet_name='Transformed_Data', index=False)
                                completeness_df.to_excel(writer, sheet_name='Quality_Report', index=False)
                            
                            st.download_button(
                                label="📊 Download Excel",
                                data=buffer.getvalue(),
                                file_name=f"hr_transformed_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True
                            )
                        
                        with col2:
                            csv_data = transformed_data.to_csv(index=False)
                            st.download_button(
                                label="📝 Download CSV",
                                data=csv_data,
                                file_name=f"hr_transformed_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
                        
                        with col3:
                            json_data = transformed_data.to_json(orient='records', indent=2)
                            st.download_button(
                                label="🔗 Download JSON",
                                data=json_data,
                                file_name=f"hr_transformed_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json",
                                mime="application/json",
                                use_container_width=True
                            )

if __name__ == "__main__":
    main()
