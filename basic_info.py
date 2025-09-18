import streamlit as st
import pandas as pd
import json
from io import BytesIO
import base64
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
import re
import numpy as np

class EnhancedDataMapper:
    """Enhanced Data Mapper that uses mapping configuration files"""
    
    def __init__(self):
        self.mapping_config = None
        self.source_data = {}
        self.lookup_tables = {}
        
    def load_mapping_config(self, mapping_df: pd.DataFrame):
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
    
    def load_source_data(self, data_dict: Dict[str, pd.DataFrame]):
        """Load all source data tables"""
        self.source_data = data_dict
        
    def load_lookup_tables(self, lookup_dict: Dict[str, pd.DataFrame]):
        """Load lookup tables for value transformations"""
        self.lookup_tables = lookup_dict
    
    def _get_value_from_source(self, person_id: str, config: Dict[str, Any]) -> Any:
        """Extract value from source data based on configuration"""
        source_table = config.get('source_table', '')
        technical_field = config.get('technical_field', '')
        source_field = config.get('source_field', '')
        notes = config.get('notes', '')
        default_value = config.get('default_value', '')
        
        # Handle different source tables
        if source_table == 'PA0002':  # Personal Data
            return self._get_pa0002_value(person_id, technical_field, source_field, notes, default_value)
        elif source_table == 'PA0105':  # Communication
            return self._get_pa0105_value(person_id, technical_field, source_field, notes, default_value)
        elif source_table == 'PA0006':  # Address
            return self._get_pa0006_value(person_id, technical_field, source_field, notes, default_value)
        elif source_table == 'PA0001':  # Organizational Assignment
            return self._get_pa0001_value(person_id, technical_field, source_field, notes, default_value)
        elif source_table == 'PA0000':  # Actions
            return self._get_pa0000_value(person_id, technical_field, source_field, notes, default_value)
        elif source_table == 'Custom':  # Custom logic
            return self._get_custom_value(person_id, technical_field, source_field, notes, default_value)
        
        return default_value if default_value else None
    
    def _get_pa0002_value(self, person_id: str, technical_field: str, source_field: str, notes: str, default_value: str) -> Any:
        """Get value from PA0002 Personal Data"""
        if 'PA0002_Personal Data' not in self.source_data:
            return default_value
        
        df = self.source_data['PA0002_Personal Data']
        person_row = df[df['PERNR'].astype(str) == str(person_id)]
        
        if person_row.empty:
            return default_value
        
        person_row = person_row.iloc[0]
        
        # Get raw value
        raw_value = person_row.get(technical_field, default_value)
        
        # Apply transformations based on notes
        return self._apply_transformations(raw_value, technical_field, notes, person_row)
    
    def _get_pa0105_value(self, person_id: str, technical_field: str, source_field: str, notes: str, default_value: str) -> Any:
        """Get value from PA0105 Communication Data"""
        if 'PA0105_Communication' not in self.source_data:
            return default_value
        
        df = self.source_data['PA0105_Communication']
        person_data = df[df['PERNR'].astype(str) == str(person_id)]
        
        if person_data.empty:
            return default_value
        
        # Extract specific communication type based on notes
        if 'email' in notes.lower() or 'SUBTY=0010' in notes:
            email_rows = person_data[
                (person_data['COMM_TYPE'] == 'EMAIL') | 
                (person_data['SUBTY'] == 10)
            ]
            if not email_rows.empty:
                return email_rows.iloc[0].get('USRID_LONG', default_value)
        
        elif 'phone' in notes.lower() or 'SUBTY=0020' in notes:
            phone_rows = person_data[
                (person_data['COMM_TYPE'] == 'PHONE') | 
                (person_data['SUBTY'] == 20)
            ]
            if not phone_rows.empty:
                return phone_rows.iloc[0].get('USRID_LONG', default_value)
        
        return default_value
    
    def _get_pa0006_value(self, person_id: str, technical_field: str, source_field: str, notes: str, default_value: str) -> Any:
        """Get value from PA0006 Address Data"""
        if 'PA0006_Home_Address' not in self.source_data:
            return default_value
        
        df = self.source_data['PA0006_Home_Address']
        person_data = df[df['PERNR'].astype(str) == str(person_id)]
        
        if person_data.empty:
            return default_value
        
        # Get home address (SUBTY=1)
        home_address = person_data[person_data['SUBTY'] == 1]
        if not home_address.empty:
            return home_address.iloc[0].get(technical_field, default_value)
        
        return default_value
    
    def _get_pa0001_value(self, person_id: str, technical_field: str, source_field: str, notes: str, default_value: str) -> Any:
        """Get value from PA0001 Organizational Assignment"""
        # This would be implemented if you have PA0001 data
        return default_value
    
    def _get_pa0000_value(self, person_id: str, technical_field: str, source_field: str, notes: str, default_value: str) -> Any:
        """Get value from PA0000 Actions"""
        # This would be implemented if you have PA0000 data
        return default_value
    
    def _get_custom_value(self, person_id: str, technical_field: str, source_field: str, notes: str, default_value: str) -> Any:
        """Handle custom logic"""
        if 'organizational assignment' in notes.lower():
            return default_value
        return default_value
    
    def _apply_transformations(self, value: Any, field_name: str, notes: str, person_row: pd.Series) -> Any:
        """Apply transformations based on notes"""
        if pd.isna(value) or value == '':
            return None
        
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
        
        # Username generation
        if 'login username' in notes.lower():
            if 'PA0105_Communication' in self.source_data:
                comm_df = self.source_data['PA0105_Communication']
                person_id = person_row.get('PERNR', '')
                login_data = comm_df[
                    (comm_df['PERNR'].astype(str) == str(person_id)) & 
                    (comm_df['COMM_TYPE'] == 'LOGIN')
                ]
                if not login_data.empty:
                    return login_data.iloc[0].get('USRID', value)
        
        # Country/Language lookups
        if field_name in self.lookup_tables:
            lookup_df = self.lookup_tables[field_name]
            # Implement lookup logic based on your lookup table structure
        
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
            raise ValueError("Mapping configuration and source data must be loaded first")
        
        # Get all unique person IDs from PA0002
        if 'PA0002_Personal Data' not in self.source_data:
            raise ValueError("PA0002_Personal Data is required as the main data source")
        
        main_df = self.source_data['PA0002_Personal Data']
        person_ids = main_df['PERNR'].astype(str).unique()
        
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
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🔄 HR Data Mapping Tool")
    st.markdown("Transform your HR data using mapping configuration files")
    
    # Initialize session state
    if 'mapper' not in st.session_state:
        st.session_state.mapper = EnhancedDataMapper()
    if 'transformed_data' not in st.session_state:
        st.session_state.transformed_data = None
    if 'mapping_loaded' not in st.session_state:
        st.session_state.mapping_loaded = False
    
    # Sidebar for file uploads
    with st.sidebar:
        st.header("📁 File Uploads")
        
        # Mapping Configuration
        st.subheader("1. Mapping Configuration")
        mapping_file = st.file_uploader(
            "Upload mapping configuration (Excel/CSV)",
            type=['xlsx', 'xls', 'csv'],
            key="mapping_file",
            help="Upload the file containing Target Column, Source Table, Technical Field mappings"
        )
        
        if mapping_file and not st.session_state.mapping_loaded:
            try:
                if mapping_file.name.endswith('.csv'):
                    mapping_df = pd.read_csv(mapping_file)
                else:
                    # Try to find the mapping sheet
                    excel_file = pd.ExcelFile(mapping_file)
                    sheet_names = excel_file.sheet_names
                    
                    # Look for mapping sheet
                    mapping_sheet = None
                    for sheet in sheet_names:
                        if 'mapping' in sheet.lower():
                            mapping_sheet = sheet
                            break
                    
                    if not mapping_sheet:
                        mapping_sheet = st.selectbox("Select Mapping Sheet:", sheet_names)
                    
                    mapping_df = pd.read_excel(mapping_file, sheet_name=mapping_sheet)
                
                st.session_state.mapper.load_mapping_config(mapping_df)
                st.session_state.mapping_loaded = True
                st.success(f"✅ Loaded {len(mapping_df)} mapping rules")
                
            except Exception as e:
                st.error(f"❌ Error loading mapping file: {str(e)}")
        
        # Source Data Files
        st.subheader("2. Source Data Files")
        
        uploaded_files = st.file_uploader(
            "Upload source data files (Excel with multiple sheets)",
            type=['xlsx', 'xls'],
            accept_multiple_files=True,
            key="source_files",
            help="Upload files containing PA0002, PA0105, PA0006 data"
        )
        
        if uploaded_files:
            source_data = {}
            
            for file in uploaded_files:
                try:
                    excel_file = pd.ExcelFile(file)
                    
                    for sheet_name in excel_file.sheet_names:
                        df = pd.read_excel(file, sheet_name=sheet_name)
                        
                        # Clean sheet name for consistent access
                        clean_sheet_name = sheet_name.strip()
                        source_data[clean_sheet_name] = df
                        
                        st.write(f"📊 {clean_sheet_name}: {len(df)} rows")
                
                except Exception as e:
                    st.error(f"❌ Error loading {file.name}: {str(e)}")
            
            if source_data:
                st.session_state.mapper.load_source_data(source_data)
                st.success(f"✅ Loaded {len(source_data)} data tables")
        
        # Lookup Tables (Optional)
        st.subheader("3. Lookup Tables (Optional)")
        lookup_files = st.file_uploader(
            "Upload lookup tables",
            type=['xlsx', 'xls', 'csv'],
            accept_multiple_files=True,
            key="lookup_files",
            help="Upload files containing language, country, or other lookup data"
        )
        
        if lookup_files:
            lookup_data = {}
            
            for file in lookup_files:
                try:
                    if file.name.endswith('.csv'):
                        df = pd.read_csv(file)
                        lookup_data[file.name.replace('.csv', '')] = df
                    else:
                        excel_file = pd.ExcelFile(file)
                        for sheet_name in excel_file.sheet_names:
                            df = pd.read_excel(file, sheet_name=sheet_name)
                            lookup_data[sheet_name] = df
                
                except Exception as e:
                    st.error(f"❌ Error loading lookup file {file.name}: {str(e)}")
            
            if lookup_data:
                st.session_state.mapper.load_lookup_tables(lookup_data)
                st.success(f"✅ Loaded {len(lookup_data)} lookup tables")
    
    # Main content area
    tab1, tab2, tab3, tab4 = st.tabs(["📋 Configuration Preview", "🔍 Data Preview", "⚡ Transform", "📊 Results"])
    
    with tab1:
        st.header("Mapping Configuration Preview")
        
        if st.session_state.mapping_loaded and st.session_state.mapper.mapping_config:
            config = st.session_state.mapper.mapping_config
            
            # Create a summary DataFrame
            config_summary = []
            for target_field, details in config.items():
                config_summary.append({
                    'Target Field': target_field,
                    'Target Name': details.get('target_name', ''),
                    'Source Table': details.get('source_table', ''),
                    'Technical Field': details.get('technical_field', ''),
                    'Transformation Notes': details.get('notes', '')[:100] + '...' if len(details.get('notes', '')) > 100 else details.get('notes', ''),
                    'Default Value': details.get('default_value', '')
                })
            
            config_df = pd.DataFrame(config_summary)
            st.dataframe(config_df, use_container_width=True, height=400)
            
            # Statistics
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Fields", len(config))
            
            source_tables = [details.get('source_table', '') for details in config.values()]
            col2.metric("Source Tables", len(set(filter(None, source_tables))))
            
            defaults = [details.get('default_value', '') for details in config.values() if details.get('default_value', '')]
            col3.metric("Fields with Defaults", len(defaults))
            
        else:
            st.info("👆 Please upload your mapping configuration file using the sidebar")
    
    with tab2:
        st.header("Source Data Preview")
        
        if hasattr(st.session_state.mapper, 'source_data') and st.session_state.mapper.source_data:
            
            # Table selector
            table_names = list(st.session_state.mapper.source_data.keys())
            selected_table = st.selectbox("Select table to preview:", table_names)
            
            if selected_table:
                df = st.session_state.mapper.source_data[selected_table]
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Rows", len(df))
                col2.metric("Columns", len(df.columns))
                col3.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")
                
                st.subheader(f"Preview: {selected_table}")
                st.dataframe(df.head(100), use_container_width=True)
                
                # Column info
                with st.expander("Column Information"):
                    col_info = pd.DataFrame({
                        'Column': df.columns,
                        'Type': df.dtypes.astype(str),
                        'Non-Null Count': df.count(),
                        'Null Count': df.isnull().sum(),
                        'Unique Values': df.nunique()
                    })
                    st.dataframe(col_info, use_container_width=True)
        
        else:
            st.info("👆 Please upload your source data files using the sidebar")
    
    with tab3:
        st.header("Data Transformation")
        
        # Check prerequisites
        can_transform = (
            st.session_state.mapping_loaded and 
            hasattr(st.session_state.mapper, 'source_data') and 
            st.session_state.mapper.source_data and
            'PA0002_Personal Data' in st.session_state.mapper.source_data
        )
        
        if can_transform:
            st.success("✅ Ready to transform data!")
            
            # Show transformation summary
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Transformation Summary")
                
                config = st.session_state.mapper.mapping_config
                main_df = st.session_state.mapper.source_data['PA0002_Personal Data']
                
                st.write(f"**Target Fields**: {len(config)} fields will be created")
                st.write(f"**Source Records**: {len(main_df)} employees will be processed")
                
                # Show source table usage
                source_tables = {}
                for details in config.values():
                    table = details.get('source_table', 'Unknown')
                    source_tables[table] = source_tables.get(table, 0) + 1
                
                st.write("**Source Table Usage:**")
                for table, count in source_tables.items():
                    available = "✅" if table in st.session_state.mapper.source_data or table == 'Custom' else "❌"
                    st.write(f"- {table}: {count} fields {available}")
            
            with col2:
                if st.button("🚀 Start Transformation", type="primary", use_container_width=True):
                    with st.spinner("Transforming data..."):
                        try:
                            transformed_df = st.session_state.mapper.transform_data()
                            st.session_state.transformed_data = transformed_df
                            st.success("✅ Data transformation completed!")
                            st.balloons()
                            
                        except Exception as e:
                            st.error(f"❌ Transformation failed: {str(e)}")
                            st.exception(e)
        
        else:
            missing_items = []
            if not st.session_state.mapping_loaded:
                missing_items.append("📋 Mapping configuration file")
            if not hasattr(st.session_state.mapper, 'source_data') or not st.session_state.mapper.source_data:
                missing_items.append("📊 Source data files")
            elif 'PA0002_Personal Data' not in st.session_state.mapper.source_data:
                missing_items.append("👤 PA0002_Personal Data (main employee data)")
            
            st.warning("⚠️ Missing required items:")
            for item in missing_items:
                st.write(f"- {item}")
    
    with tab4:
        st.header("Transformation Results")
        
        if st.session_state.transformed_data is not None:
            transformed_df = st.session_state.transformed_data
            
            # Results summary
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Employees Processed", len(transformed_df))
            col2.metric("Fields Generated", len(transformed_df.columns))
            
            # Calculate completion rate
            total_fields = len(transformed_df.columns) * len(transformed_df)
            filled_fields = transformed_df.count().sum()
            completion_rate = (filled_fields / total_fields * 100) if total_fields > 0 else 0
            col3.metric("Data Completion", f"{completion_rate:.1f}%")
            
            # Memory usage
            memory_mb = transformed_df.memory_usage(deep=True).sum() / 1024 / 1024
            col4.metric("Memory Usage", f"{memory_mb:.1f} MB")
            
            # Data preview
            st.subheader("Transformed Data Preview")
            st.dataframe(transformed_df.head(50), use_container_width=True)
            
            # Export options
            st.subheader("📥 Export Options")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Excel export
                buffer = BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    transformed_df.to_excel(writer, sheet_name='Transformed_Data', index=False)
                    
                    # Add summary sheet
                    summary_data = {
                        'Metric': ['Total Employees', 'Total Fields', 'Data Completion Rate', 'Transformation Date'],
                        'Value': [len(transformed_df), len(transformed_df.columns), f"{completion_rate:.1f}%", pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')]
                    }
                    summary_df = pd.DataFrame(summary_data)
                    summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                st.download_button(
                    label="📊 Download Excel",
                    data=buffer.getvalue(),
                    file_name=f"hr_transformed_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            
            with col2:
                # CSV export
                csv_data = transformed_df.to_csv(index=False)
                st.download_button(
                    label="📄 Download CSV",
                    data=csv_data,
                    file_name=f"hr_transformed_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            with col3:
                # JSON export
                json_data = transformed_df.to_json(orient='records', indent=2)
                st.download_button(
                    label="📋 Download JSON",
                    data=json_data,
                    file_name=f"hr_transformed_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json",
                    use_container_width=True
                )
            
            # Data Quality Report
            with st.expander("📊 Data Quality Report", expanded=False):
                
                # Missing data analysis
                missing_data = transformed_df.isnull().sum()
                missing_pct = (missing_data / len(transformed_df) * 100).round(2)
                
                quality_df = pd.DataFrame({
                    'Field': missing_data.index,
                    'Missing Count': missing_data.values,
                    'Missing %': missing_pct.values,
                    'Filled Count': len(transformed_df) - missing_data.values
                }).sort_values('Missing %', ascending=False)
                
                st.subheader("Field Completion Analysis")
                st.dataframe(quality_df, use_container_width=True)
                
                # Unique values analysis for key fields
                key_fields = ['userId', 'email', 'username', 'firstName', 'lastName']
                available_key_fields = [field for field in key_fields if field in transformed_df.columns]
                
                if available_key_fields:
                    st.subheader("Key Fields Analysis")
                    key_analysis = []
                    
                    for field in available_key_fields:
                        unique_count = transformed_df[field].nunique()
                        duplicate_count = len(transformed_df) - unique_count
                        key_analysis.append({
                            'Field': field,
                            'Unique Values': unique_count,
                            'Duplicates': duplicate_count,
                            'Uniqueness %': (unique_count / len(transformed_df) * 100).round(2) if len(transformed_df) > 0 else 0
                        })
                    
                    key_analysis_df = pd.DataFrame(key_analysis)
                    st.dataframe(key_analysis_df, use_container_width=True)
        
        else:
            st.info("👆 Transform your data first to see results")
    
    # Footer
    st.markdown("---")
    st.markdown(
        "🔧 **HR Data Mapping Tool** | Built with Streamlit | "
        "Upload your mapping configuration and source data to get started"
    )

if __name__ == "__main__":
    main()
