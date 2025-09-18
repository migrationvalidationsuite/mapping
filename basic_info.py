import streamlit as st
import pandas as pd
import json
from io import BytesIO
import base64
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
import re
import numpy as np

class SingleFileDataMapper:
    """Simplified Data Mapper that works with a single Excel file containing all data and mapping"""
    
    def __init__(self):
        self.mapping_config = None
        self.source_data = {}
        self.all_sheets = {}
        
    def load_excel_file(self, excel_file):
        """Load all sheets from a single Excel file"""
        try:
            excel_data = pd.ExcelFile(excel_file)
            self.all_sheets = {}
            
            for sheet_name in excel_data.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                clean_name = sheet_name.strip()
                self.all_sheets[clean_name] = df
            
            # Auto-detect mapping sheet and data sheets
            self._auto_detect_sheets()
            return True
            
        except Exception as e:
            st.error(f"Error loading Excel file: {str(e)}")
            return False
    
    def _auto_detect_sheets(self):
        """Auto-detect which sheet is mapping and which are data sheets"""
        mapping_sheet = None
        
        # Look for mapping sheet
        for sheet_name, df in self.all_sheets.items():
            # Check if this sheet has mapping columns
            columns = [col.lower() for col in df.columns]
            if any('target column' in col for col in columns) and any('source table' in col for col in columns):
                mapping_sheet = sheet_name
                break
        
        if mapping_sheet:
            self._load_mapping_config(self.all_sheets[mapping_sheet])
            
            # Load data sheets (everything except mapping)
            for sheet_name, df in self.all_sheets.items():
                if sheet_name != mapping_sheet:
                    self.source_data[sheet_name] = df
    
    def _load_mapping_config(self, mapping_df: pd.DataFrame):
        """Load mapping configuration from DataFrame"""
        self.mapping_config = {}
        
        for _, row in mapping_df.iterrows():
            target_field = row.get('Target Column (SuccessFactors)', '')
            if pd.notna(target_field) and target_field.strip():
                self.mapping_config[target_field] = {
                    'target_name': row.get('Target Column Name', ''),
                    'source_table': row.get('Source Table (ECC)', ''),
                    'source_field': row.get('Source Field Name (ECC)', ''),
                    'technical_field': row.get('Technical Field (ECC)', ''),
                    'notes': row.get('Notes / Transformation', ''),
                    'default_value': row.get('Default Value', '')
                }
    
    def _find_data_sheet(self, source_table: str) -> Optional[pd.DataFrame]:
        """Find the data sheet that corresponds to the source table"""
        # Direct match
        if source_table in self.source_data:
            return self.source_data[source_table]
        
        # Try to find by pattern matching
        for sheet_name, df in self.source_data.items():
            if source_table in sheet_name or sheet_name in source_table:
                return df
        
        return None
    
    def _get_value_from_source(self, person_id: str, config: Dict[str, Any]) -> Any:
        """Extract value from source data based on configuration"""
        source_table = config.get('source_table', '')
        technical_field = config.get('technical_field', '')
        notes = config.get('notes', '')
        default_value = config.get('default_value', '')
        
        # Find the appropriate data sheet
        data_df = self._find_data_sheet(source_table)
        if data_df is None:
            return default_value
        
        # Find person's record
        person_records = data_df[data_df['PERNR'].astype(str) == str(person_id)]
        if person_records.empty:
            return default_value
        
        # Handle specific logic based on source table
        if source_table == 'PA0105':  # Communication data
            return self._get_communication_value(person_records, technical_field, notes, default_value)
        elif source_table == 'PA0006':  # Address data
            return self._get_address_value(person_records, technical_field, notes, default_value)
        else:  # PA0002 or other personal data
            return self._get_personal_value(person_records, technical_field, notes, default_value)
    
    def _get_communication_value(self, person_records: pd.DataFrame, technical_field: str, notes: str, default_value: str) -> Any:
        """Get communication data (email, phone)"""
        if 'email' in notes.lower() or 'SUBTY=0010' in notes:
            email_records = person_records[person_records['SUBTY'] == 10]
            if not email_records.empty:
                return email_records.iloc[0].get('USRID_LONG', default_value)
        elif 'phone' in notes.lower() or 'SUBTY=0020' in notes:
            phone_records = person_records[person_records['SUBTY'] == 20]
            if not phone_records.empty:
                return phone_records.iloc[0].get('USRID_LONG', default_value)
        
        return default_value
    
    def _get_address_value(self, person_records: pd.DataFrame, technical_field: str, notes: str, default_value: str) -> Any:
        """Get address data"""
        # Get home address (SUBTY=1)
        home_records = person_records[person_records['SUBTY'] == 1]
        if not home_records.empty:
            return home_records.iloc[0].get(technical_field, default_value)
        return default_value
    
    def _get_personal_value(self, person_records: pd.DataFrame, technical_field: str, notes: str, default_value: str) -> Any:
        """Get personal data with transformations"""
        person_row = person_records.iloc[0]
        raw_value = person_row.get(technical_field, default_value)
        
        # Apply transformations
        return self._apply_transformations(raw_value, technical_field, notes, person_row, default_value)
    
    def _apply_transformations(self, value: Any, field_name: str, notes: str, person_row: pd.Series, default_value: str) -> Any:
        """Apply transformations based on notes"""
        if pd.isna(value) or value == '':
            return default_value if default_value else None
        
        # Gender transformations
        if 'gender' in field_name.lower() or 'GESCH' in field_name:
            gender_map = {'1': 'Male', '2': 'Female', 'M': 'Male', 'F': 'Female'}
            return gender_map.get(str(value), value)
        
        # Date transformations
        if 'date' in notes.lower() or 'GBDAT' in field_name or 'BEGDA' in field_name:
            return self._format_date(value)
        
        # Marital Status transformations
        if 'marital' in notes.lower() or 'FAMST' in field_name:
            marital_map = {
                '0': 'Single', '1': 'Married', '2': 'Divorced', 
                '3': 'Widowed', '4': 'Separated'
            }
            return marital_map.get(str(value), value)
        
        # Display name concatenation
        if 'concatenate VORNA + NACHN' in notes:
            first_name = person_row.get('VORNA', '')
            last_name = person_row.get('NACHN', '')
            return f"{first_name} {last_name}".strip()
        
        return value
    
    def _format_date(self, date_value: Any) -> str:
        """Format date from YYYYMMDD to yyyy-mm-dd"""
        if pd.isna(date_value):
            return None
        
        date_str = str(date_value)
        if len(date_str) == 8 and date_str.isdigit():
            try:
                year = date_str[:4]
                month = date_str[4:6]
                day = date_str[6:8]
                return f"{year}-{month}-{day}"
            except:
                return date_str
        
        return date_str
    
    def transform_data(self) -> pd.DataFrame:
        """Transform data according to mapping configuration"""
        if not self.mapping_config or not self.source_data:
            raise ValueError("Excel file must be loaded first")
        
        # Find the main personal data sheet
        main_data_sheet = None
        for sheet_name, df in self.source_data.items():
            if 'PA0002' in sheet_name or 'Personal' in sheet_name:
                main_data_sheet = df
                break
        
        if main_data_sheet is None:
            raise ValueError("Could not find main personal data sheet (PA0002)")
        
        # Get all unique person IDs
        person_ids = main_data_sheet['PERNR'].astype(str).unique()
        
        # Build result data
        result_rows = []
        
        for person_id in person_ids:
            row_data = {}
            
            # Process each target field from mapping configuration
            for target_field, config in self.mapping_config.items():
                value = self._get_value_from_source(person_id, config)
                row_data[target_field] = value
            
            result_rows.append(row_data)
        
        return pd.DataFrame(result_rows)


def main():
    st.set_page_config(
        page_title="HR Data Mapping Tool",
        page_icon="🔄",
        layout="wide"
    )
    
    st.title("🔄 HR Data Mapping Tool")
    st.markdown("**Simple one-file solution**: Upload your Excel file with mapping configuration and data sheets")
    
    # Initialize session state
    if 'mapper' not in st.session_state:
        st.session_state.mapper = SingleFileDataMapper()
    if 'file_loaded' not in st.session_state:
        st.session_state.file_loaded = False
    if 'transformed_data' not in st.session_state:
        st.session_state.transformed_data = None
    
    # Single file upload
    st.header("📁 Upload Your Excel File")
    st.markdown("Upload the Excel file that contains both your mapping configuration and all data sheets (PA0002, PA0105, PA0006, etc.)")
    
    uploaded_file = st.file_uploader(
        "Choose Excel file",
        type=['xlsx', 'xls'],
        key="excel_file",
        help="Excel file should contain: Mapping sheet + PA0002_Personal Data + PA0105_Communication + PA0006_Home_Address + any lookup tables"
    )
    
    if uploaded_file and not st.session_state.file_loaded:
        with st.spinner("Loading Excel file..."):
            if st.session_state.mapper.load_excel_file(uploaded_file):
                st.session_state.file_loaded = True
                st.success("✅ Excel file loaded successfully!")
                st.rerun()
    
    if st.session_state.file_loaded:
        # Show file summary
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Sheets", len(st.session_state.mapper.all_sheets))
        col2.metric("Data Sheets", len(st.session_state.mapper.source_data))
        col3.metric("Mapping Rules", len(st.session_state.mapper.mapping_config) if st.session_state.mapper.mapping_config else 0)
        
        # Tabs for different views
        tab1, tab2, tab3, tab4 = st.tabs(["📋 File Overview", "🔍 Data Preview", "⚡ Transform", "📊 Results"])
        
        with tab1:
            st.header("File Overview")
            
            # Show all sheets
            st.subheader("All Sheets in File")
            sheet_info = []
            for sheet_name, df in st.session_state.mapper.all_sheets.items():
                sheet_type = "Mapping" if sheet_name not in st.session_state.mapper.source_data else "Data"
                sheet_info.append({
                    'Sheet Name': sheet_name,
                    'Type': sheet_type,
                    'Rows': len(df),
                    'Columns': len(df.columns),
                    'Sample Columns': ', '.join(df.columns[:3].tolist()) + ('...' if len(df.columns) > 3 else '')
                })
            
            sheet_df = pd.DataFrame(sheet_info)
            st.dataframe(sheet_df, use_container_width=True)
            
            # Show mapping configuration summary
            if st.session_state.mapper.mapping_config:
                st.subheader("Mapping Configuration Summary")
                
                # Group by source table
                source_tables = {}
                for target_field, config in st.session_state.mapper.mapping_config.items():
                    table = config.get('source_table', 'Unknown')
                    if table not in source_tables:
                        source_tables[table] = []
                    source_tables[table].append(target_field)
                
                for table, fields in source_tables.items():
                    with st.expander(f"{table} ({len(fields)} fields)"):
                        st.write(", ".join(fields))
        
        with tab2:
            st.header("Data Preview")
            
            if st.session_state.mapper.source_data:
                # Sheet selector
                sheet_names = list(st.session_state.mapper.source_data.keys())
                selected_sheet = st.selectbox("Select sheet to preview:", sheet_names)
                
                if selected_sheet:
                    df = st.session_state.mapper.source_data[selected_sheet]
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Rows", len(df))
                    col2.metric("Columns", len(df.columns))
                    
                    # Show unique person count if PERNR exists
                    if 'PERNR' in df.columns:
                        unique_persons = df['PERNR'].nunique()
                        col3.metric("Unique Persons", unique_persons)
                    
                    st.subheader(f"Preview: {selected_sheet}")
                    st.dataframe(df.head(20), use_container_width=True)
        
        with tab3:
            st.header("Transform Data")
            
            # Check if ready to transform
            can_transform = (
                st.session_state.mapper.mapping_config and 
                st.session_state.mapper.source_data and
                any('PA0002' in sheet or 'Personal' in sheet for sheet in st.session_state.mapper.source_data.keys())
            )
            
            if can_transform:
                st.success("✅ Ready to transform!")
                
                # Show what will be transformed
                config = st.session_state.mapper.mapping_config
                st.write(f"**Will create {len(config)} target fields**")
                
                # Find main data sheet for person count
                main_sheet = None
                for sheet_name, df in st.session_state.mapper.source_data.items():
                    if 'PA0002' in sheet_name or 'Personal' in sheet_name:
                        main_sheet = df
                        break
                
                if main_sheet is not None:
                    person_count = main_sheet['PERNR'].nunique()
                    st.write(f"**Will process {person_count} employees**")
                
                # Transform button
                if st.button("🚀 Transform Data", type="primary", use_container_width=True):
                    with st.spinner("Transforming data..."):
                        try:
                            transformed_df = st.session_state.mapper.transform_data()
                            st.session_state.transformed_data = transformed_df
                            st.success(f"✅ Transformation completed! Generated {len(transformed_df)} rows with {len(transformed_df.columns)} fields")
                            st.balloons()
                            
                        except Exception as e:
                            st.error(f"❌ Transformation failed: {str(e)}")
                            
            else:
                st.warning("⚠️ Cannot transform - missing required data or mapping configuration")
        
        with tab4:
            st.header("Results")
            
            if st.session_state.transformed_data is not None:
                transformed_df = st.session_state.transformed_data
                
                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Employees", len(transformed_df))
                col2.metric("Fields", len(transformed_df.columns))
                
                # Data completeness
                total_cells = len(transformed_df) * len(transformed_df.columns)
                filled_cells = transformed_df.count().sum()
                completeness = (filled_cells / total_cells * 100) if total_cells > 0 else 0
                col3.metric("Completeness", f"{completeness:.1f}%")
                
                # File size estimate
                memory_mb = transformed_df.memory_usage(deep=True).sum() / 1024 / 1024
                col4.metric("Size", f"{memory_mb:.1f} MB")
                
                # Data preview
                st.subheader("Transformed Data Preview")
                st.dataframe(transformed_df.head(20), use_container_width=True)
                
                # Export options
                st.subheader("📥 Download Results")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    # Excel export
                    buffer = BytesIO()
                    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                        transformed_df.to_excel(writer, sheet_name='Transformed_Data', index=False)
                    
                    st.download_button(
                        label="📊 Download Excel",
                        data=buffer.getvalue(),
                        file_name=f"hr_transformed_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                
                with col2:
                    # CSV export
                    csv_data = transformed_df.to_csv(index=False)
                    st.download_button(
                        label="📄 Download CSV",
                        data=csv_data,
                        file_name=f"hr_transformed_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                
                with col3:
                    # JSON export
                    json_data = transformed_df.to_json(orient='records', indent=2)
                    st.download_button(
                        label="📋 Download JSON",
                        data=json_data,
                        file_name=f"hr_transformed_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json",
                        use_container_width=True
                    )
                
                # Data quality report
                with st.expander("📊 Data Quality Report"):
                    missing_data = transformed_df.isnull().sum()
                    missing_pct = (missing_data / len(transformed_df) * 100).round(2)
                    
                    quality_df = pd.DataFrame({
                        'Field': missing_data.index,
                        'Missing Count': missing_data.values,
                        'Missing %': missing_pct.values,
                        'Completed Count': len(transformed_df) - missing_data.values
                    }).sort_values('Missing %', ascending=False)
                    
                    st.dataframe(quality_df, use_container_width=True)
            
            else:
                st.info("👆 Transform your data first to see results")
    
    else:
        st.info("👆 Upload your Excel file to get started")
    
    # Instructions
    with st.expander("📖 How to use this tool"):
        st.markdown("""
        **Step 1**: Prepare your Excel file with:
        - **Mapping sheet**: Contains Target Column, Source Table, Technical Field columns
        - **Data sheets**: PA0002_Personal Data, PA0105_Communication, PA0006_Home_Address, etc.
        - **Lookup sheets** (optional): Language codes, country codes, etc.
        
        **Step 2**: Upload the single Excel file using the file uploader above
        
        **Step 3**: The tool will automatically:
        - Detect the mapping configuration
        - Load all data sheets
        - Show you a preview of what will be transformed
        
        **Step 4**: Click "Transform Data" to generate the output
        
        **Step 5**: Download the results in your preferred format (Excel, CSV, JSON)
        
        That's it! One file upload, automatic processing, complete results.
        """)

if __name__ == "__main__":
    main()
