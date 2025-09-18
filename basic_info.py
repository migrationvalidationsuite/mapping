import streamlit as st
import pandas as pd
import json
from io import BytesIO
import base64
from typing import Dict, List, Any, Optional
from collections import defaultdict
import re

# Import your FlexibleDataMapper class here
# from flexible_data_mapper import FlexibleDataMapper

# For this demo, I'll include a simplified version of the mapper
class FlexibleDataMapper:
    """Simplified version of the FlexibleDataMapper for Streamlit demo"""
    
    def __init__(self):
        self.field_aliases = self._initialize_field_aliases()
        self.value_mappings = self._initialize_value_mappings()
        self.lookup_tables = {}
        
    def _initialize_field_aliases(self) -> Dict[str, List[str]]:
        """Initialize field name aliases to handle variations in column names."""
        return {
            'userId': ['user_id', 'userid', 'employee_id', 'emp_id', 'id', 'personnel_number', 'pernr'],
            'username': ['user_name', 'login', 'login_id', 'user_login', 'account', 'uid'],
            'firstName': ['first_name', 'fname', 'given_name', 'forename', 'vorna'],
            'middleName': ['middle_name', 'mname', 'middle_initial', 'midnm'],
            'lastName': ['last_name', 'lname', 'surname', 'family_name', 'nachn'],
            'displayName': ['display_name', 'full_name', 'name', 'rufnm', 'known_as'],
            'gender': ['sex', 'gender_key', 'gender_code', 'gesch'],
            'dateOfBirth': ['dob', 'birth_date', 'birthdate', 'date_birth', 'gbdat'],
            'maritalStatus': ['marital_status', 'marriage_status', 'famst'],
            'nationality': ['country_of_origin', 'citizenship', 'natio'],
            'email': ['email_address', 'work_email', 'business_email', 'mail'],
            'businessPhone': ['phone', 'work_phone', 'office_phone', 'business_phone_number'],
            'manager': ['supervisor', 'line_manager', 'reporting_manager', 'boss'],
            'department': ['dept', 'division', 'organizational_unit', 'orgeh'],
            'jobCode': ['job_title', 'position', 'role', 'stell'],
            'location': ['work_location', 'office', 'site', 'btrtl'],
            'hireDate': ['start_date', 'employment_date', 'join_date', 'begda'],
            'status': ['employment_status', 'employee_status', 'active_status', 'stat2'],
            'addressLine1': ['address1', 'street', 'street_address', 'stras'],
            'addressLine2': ['address2', 'apt', 'unit', 'suite', 'locat'],
            'city': ['town', 'municipality', 'ort01'],
            'state': ['province', 'region', 'state_province'],
            'zipCode': ['postal_code', 'zip', 'postcode', 'pstlz'],
            'country': ['country_code', 'nation', 'land1'],
            'defaultLocale': ['locale', 'language', 'lang', 'spras'],
            'timeZone': ['timezone', 'tz'],
            'hr': ['hr_rep', 'hr_representative', 'hr_contact']
        }
    
    def _initialize_value_mappings(self) -> Dict[str, Dict[str, str]]:
        """Initialize value mappings for standardizing field values."""
        return {
            'gender': {
                '1': 'Male', 'M': 'Male', 'MALE': 'Male', 'male': 'Male',
                '2': 'Female', 'F': 'Female', 'FEMALE': 'Female', 'female': 'Female',
                '0': 'Not Specified', 'X': 'Not Specified', 'OTHER': 'Other',
                'rather not say': 'Not Specified', 'prefer not to say': 'Not Specified',
                'non-binary': 'Other', 'nb': 'Other'
            },
            'maritalStatus': {
                '0': 'Single', '1': 'Married', '2': 'Divorced', '3': 'Widowed',
                'S': 'Single', 'M': 'Married', 'D': 'Divorced', 'W': 'Widowed',
                'single': 'Single', 'married': 'Married', 'divorced': 'Divorced',
                'widowed': 'Widowed', 'separated': 'Separated'
            },
            'status': {
                'A': 'Active', 'I': 'Inactive', 'T': 'Terminated', 'L': 'Leave',
                'active': 'Active', 'inactive': 'Inactive', 'terminated': 'Terminated',
                '1': 'Active', '0': 'Inactive', '2': 'Active'
            }
        }
    
    def _normalize_field_name(self, field_name: str) -> Optional[str]:
        """Normalize field name to standard format."""
        if not field_name:
            return None
            
        field_lower = field_name.lower().strip()
        
        for standard_name, aliases in self.field_aliases.items():
            if field_lower == standard_name.lower() or field_lower in [alias.lower() for alias in aliases]:
                return standard_name
        
        return field_name
    
    def _map_field_value(self, field_name: str, value: Any) -> Any:
        """Map field values using predefined mappings."""
        if pd.isna(value) or value == '':
            return None
            
        str_value = str(value).strip()
        
        if field_name in self.value_mappings:
            mapping = self.value_mappings[field_name]
            return mapping.get(str_value, mapping.get(str_value.lower(), str_value))
        
        return str_value
    
    def transform_data(self, main_df: pd.DataFrame, comm_df: Optional[pd.DataFrame] = None, 
                      addr_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Transform the input data according to the mapping configuration."""
        result_rows = []
        
        for _, row in main_df.iterrows():
            transformed_row = {}
            
            for col in main_df.columns:
                normalized_field = self._normalize_field_name(col)
                if normalized_field:
                    raw_value = row[col]
                    mapped_value = self._map_field_value(normalized_field, raw_value)
                    transformed_row[normalized_field] = mapped_value
            
            result_rows.append(transformed_row)
        
        return pd.DataFrame(result_rows)


def main():
    st.set_page_config(
        page_title="HR Data Mapping Tool",
        page_icon="🔄",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🔄 HR Data Mapping Tool")
    st.markdown("Transform your HR data with flexible field and value mappings")
    
    # Initialize session state
    if 'mapper' not in st.session_state:
        st.session_state.mapper = FlexibleDataMapper()
    if 'transformed_data' not in st.session_state:
        st.session_state.transformed_data = None
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Field Aliases Management
        with st.expander("Field Aliases", expanded=False):
            st.write("Add custom field name variations:")
            
            col1, col2 = st.columns(2)
            with col1:
                standard_field = st.selectbox(
                    "Standard Field:", 
                    list(st.session_state.mapper.field_aliases.keys())
                )
            with col2:
                new_alias = st.text_input("New Alias:")
            
            if st.button("Add Alias") and new_alias:
                if new_alias not in st.session_state.mapper.field_aliases[standard_field]:
                    st.session_state.mapper.field_aliases[standard_field].append(new_alias.lower())
                    st.success(f"Added '{new_alias}' as alias for '{standard_field}'")
                    st.rerun()
        
        # Value Mappings Management
        with st.expander("Value Mappings", expanded=False):
            st.write("Add custom value mappings:")
            
            field_for_mapping = st.selectbox(
                "Field for Value Mapping:", 
                list(st.session_state.mapper.value_mappings.keys())
            )
            
            col1, col2 = st.columns(2)
            with col1:
                source_value = st.text_input("Source Value:")
            with col2:
                target_value = st.text_input("Target Value:")
            
            if st.button("Add Value Mapping") and source_value and target_value:
                if field_for_mapping not in st.session_state.mapper.value_mappings:
                    st.session_state.mapper.value_mappings[field_for_mapping] = {}
                st.session_state.mapper.value_mappings[field_for_mapping][source_value] = target_value
                st.success(f"Added mapping: {source_value} → {target_value}")
                st.rerun()
        
        # Export/Import Configuration
        with st.expander("Configuration Management", expanded=False):
            if st.button("Export Configuration"):
                config = {
                    'field_aliases': st.session_state.mapper.field_aliases,
                    'value_mappings': st.session_state.mapper.value_mappings
                }
                config_json = json.dumps(config, indent=2)
                st.download_button(
                    label="Download Config JSON",
                    data=config_json,
                    file_name="mapping_config.json",
                    mime="application/json"
                )
            
            uploaded_config = st.file_uploader("Import Configuration", type=['json'])
            if uploaded_config:
                config = json.load(uploaded_config)
                st.session_state.mapper.field_aliases.update(config.get('field_aliases', {}))
                st.session_state.mapper.value_mappings.update(config.get('value_mappings', {}))
                st.success("Configuration imported successfully!")
                st.rerun()
    
    # Main content area
    tab1, tab2, tab3, tab4 = st.tabs(["📁 Data Upload", "🔍 Preview & Mapping", "⚡ Transform", "📊 Results"])
    
    with tab1:
        st.header("Upload Your Data Files")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.subheader("Main Employee Data")
            main_file = st.file_uploader(
                "Upload main employee data (Excel/CSV)", 
                type=['xlsx', 'xls', 'csv'],
                key="main_file"
            )
            
            if main_file:
                try:
                    if main_file.name.endswith('.csv'):
                        main_df = pd.read_csv(main_file)
                    else:
                        # For Excel files, let user select sheet
                        excel_file = pd.ExcelFile(main_file)
                        sheet_names = excel_file.sheet_names
                        selected_sheet = st.selectbox("Select Sheet:", sheet_names, key="main_sheet")
                        main_df = pd.read_excel(main_file, sheet_name=selected_sheet)
                    
                    st.session_state.main_df = main_df
                    st.success(f"Loaded {len(main_df)} rows")
                    
                except Exception as e:
                    st.error(f"Error loading file: {str(e)}")
        
        with col2:
            st.subheader("Communication Data (Optional)")
            comm_file = st.file_uploader(
                "Upload communication data", 
                type=['xlsx', 'xls', 'csv'],
                key="comm_file"
            )
            
            if comm_file:
                try:
                    if comm_file.name.endswith('.csv'):
                        comm_df = pd.read_csv(comm_file)
                    else:
                        excel_file = pd.ExcelFile(comm_file)
                        sheet_names = excel_file.sheet_names
                        selected_sheet = st.selectbox("Select Sheet:", sheet_names, key="comm_sheet")
                        comm_df = pd.read_excel(comm_file, sheet_name=selected_sheet)
                    
                    st.session_state.comm_df = comm_df
                    st.success(f"Loaded {len(comm_df)} rows")
                    
                except Exception as e:
                    st.error(f"Error loading file: {str(e)}")
        
        with col3:
            st.subheader("Address Data (Optional)")
            addr_file = st.file_uploader(
                "Upload address data", 
                type=['xlsx', 'xls', 'csv'],
                key="addr_file"
            )
            
            if addr_file:
                try:
                    if addr_file.name.endswith('.csv'):
                        addr_df = pd.read_csv(addr_file)
                    else:
                        excel_file = pd.ExcelFile(addr_file)
                        sheet_names = excel_file.sheet_names
                        selected_sheet = st.selectbox("Select Sheet:", sheet_names, key="addr_sheet")
                        addr_df = pd.read_excel(addr_file, sheet_name=selected_sheet)
                    
                    st.session_state.addr_df = addr_df
                    st.success(f"Loaded {len(addr_df)} rows")
                    
                except Exception as e:
                    st.error(f"Error loading file: {str(e)}")
    
    with tab2:
        st.header("Preview Data & Field Mappings")
        
        if 'main_df' in st.session_state:
            main_df = st.session_state.main_df
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Original Data Sample")
                st.dataframe(main_df.head(), use_container_width=True)
                
                st.subheader("Original Columns")
                original_cols = list(main_df.columns)
                st.write(original_cols)
            
            with col2:
                st.subheader("Field Mappings Preview")
                mapping_preview = {}
                
                for col in main_df.columns:
                    normalized = st.session_state.mapper._normalize_field_name(col)
                    mapping_preview[col] = normalized if normalized != col else "No change"
                
                mapping_df = pd.DataFrame(list(mapping_preview.items()), 
                                        columns=['Original Field', 'Mapped Field'])
                st.dataframe(mapping_df, use_container_width=True)
                
                # Show value mappings for sample data
                st.subheader("Value Mapping Preview")
                sample_row = main_df.iloc[0] if len(main_df) > 0 else None
                
                if sample_row is not None:
                    for col in main_df.columns[:5]:  # Show first 5 columns
                        normalized_field = st.session_state.mapper._normalize_field_name(col)
                        original_value = sample_row[col]
                        mapped_value = st.session_state.mapper._map_field_value(normalized_field, original_value)
                        
                        if str(original_value) != str(mapped_value):
                            st.write(f"**{col}**: {original_value} → {mapped_value}")
        else:
            st.info("Please upload your main data file first.")
    
    with tab3:
        st.header("Transform Data")
        
        if 'main_df' in st.session_state:
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.subheader("Transformation Options")
                
                include_comm = st.checkbox("Include Communication Data", 
                                         value='comm_df' in st.session_state)
                include_addr = st.checkbox("Include Address Data", 
                                         value='addr_df' in st.session_state)
                
                if st.button("🚀 Transform Data", type="primary"):
                    with st.spinner("Transforming data..."):
                        try:
                            main_df = st.session_state.main_df
                            comm_df = st.session_state.get('comm_df') if include_comm else None
                            addr_df = st.session_state.get('addr_df') if include_addr else None
                            
                            transformed_df = st.session_state.mapper.transform_data(
                                main_df, comm_df, addr_df
                            )
                            
                            st.session_state.transformed_data = transformed_df
                            st.success("Data transformed successfully!")
                            
                        except Exception as e:
                            st.error(f"Error during transformation: {str(e)}")
            
            with col2:
                if st.session_state.transformed_data is not None:
                    st.subheader("Transformation Summary")
                    
                    original_cols = len(st.session_state.main_df.columns)
                    transformed_cols = len(st.session_state.transformed_data.columns)
                    rows_count = len(st.session_state.transformed_data)
                    
                    col_a, col_b, col_c = st.columns(3)
                    col_a.metric("Original Columns", original_cols)
                    col_b.metric("Transformed Columns", transformed_cols)
                    col_c.metric("Total Rows", rows_count)
        else:
            st.info("Please upload your data files first.")
    
    with tab4:
        st.header("Results & Export")
        
        if st.session_state.transformed_data is not None:
            transformed_df = st.session_state.transformed_data
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.subheader("Transformed Data Preview")
                st.dataframe(transformed_df, use_container_width=True)
            
            with col2:
                st.subheader("Export Options")
                
                # Excel export
                buffer = BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    transformed_df.to_excel(writer, sheet_name='Transformed_Data', index=False)
                
                st.download_button(
                    label="📥 Download as Excel",
                    data=buffer.getvalue(),
                    file_name="transformed_hr_data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
                # CSV export
                csv_data = transformed_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv_data,
                    file_name="transformed_hr_data.csv",
                    mime="text/csv"
                )
                
                # JSON export
                json_data = transformed_df.to_json(orient='records', indent=2)
                st.download_button(
                    label="📥 Download as JSON",
                    data=json_data,
                    file_name="transformed_hr_data.json",
                    mime="application/json"
                )
                
                st.subheader("Data Quality Report")
                
                # Missing values report
                missing_counts = transformed_df.isnull().sum()
                missing_pct = (missing_counts / len(transformed_df) * 100).round(2)
                
                quality_df = pd.DataFrame({
                    'Field': missing_counts.index,
                    'Missing Count': missing_counts.values,
                    'Missing %': missing_pct.values
                })
                
                quality_df = quality_df[quality_df['Missing Count'] > 0].sort_values('Missing %', ascending=False)
                
                if not quality_df.empty:
                    st.subheader("Missing Data Report")
                    st.dataframe(quality_df, use_container_width=True)
                else:
                    st.success("No missing data detected!")
        else:
            st.info("Transform your data first to see results.")
    
    # Footer
    st.markdown("---")
    st.markdown(
        "Built with ❤️ using Streamlit | "
        "For support, contact your IT team"
    )

if __name__ == "__main__":
    main()
