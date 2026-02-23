# Third-Party Notices

Performance Monitor includes integrations with the following third-party open-source software components. Each component is subject to the license terms specified below.

---

## sp_WhoIsActive

**Author**: Adam Machanic
**Repository**: https://github.com/amachanic/sp_whoisactive
**License**: GNU General Public License v3.0 (GPLv3)

sp_WhoIsActive is used for real-time query activity monitoring and capturing query snapshots.

### License Text

Copyright (C) Adam Machanic

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

Full license: https://github.com/amachanic/sp_whoisactive/blob/master/LICENSE

---

## DarlingData

**Author**: Erik Darling (Darling Data, LLC)
**Repository**: https://github.com/erikdarlingdata/DarlingData
**License**: MIT License

DarlingData provides sp_HealthParser for system health analysis and sp_HumanEventsBlockViewer for blocking event analysis.

### License Text

MIT License

Copyright (c) 2025 Erik Darling

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

Full license: https://github.com/erikdarlingdata/DarlingData/blob/main/LICENSE.md

---

## SQL Server First Responder Kit

**Author**: Brent Ozar Unlimited
**Repository**: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit
**License**: MIT License

The First Responder Kit provides sp_BlitzLock for deadlock analysis and other SQL Server diagnostic tools.

### License Text

MIT License

Copyright (c) 2025 Brent Ozar Unlimited

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

Full license: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/blob/main/LICENSE.md

---

## Notice Regarding GPLv3 Component

Performance Monitor includes an **optional integration** with sp_WhoIsActive, which is licensed under GPLv3. This integration:

- Is **optional**: Performance Monitor functions without sp_WhoIsActive installed
- Does not modify sp_WhoIsActive source code
- Calls sp_WhoIsActive as an external tool when available
- Does not create a derivative work of sp_WhoIsActive

Users who choose to install sp_WhoIsActive must comply with the GPLv3 license terms for that component. Performance Monitor itself remains under its separate license.

If you have concerns about GPLv3 compatibility in your environment, you can:
1. Skip sp_WhoIsActive installation during setup
2. Use alternative query monitoring approaches
3. Contact Darling Data, LLC for guidance

---

## vscode-mssql (Execution Plan Icons)

**Author**: Microsoft Corporation
**Repository**: https://github.com/microsoft/vscode-mssql
**License**: MIT License

Execution plan operator icons (PNG) from the vscode-mssql extension are used in the Plan Viewer feature.

### License Text

MIT License

Copyright (c) Microsoft Corporation

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

Full license: https://github.com/microsoft/vscode-mssql/blob/main/LICENSE

---

## Acknowledgments

Performance Monitor would not be possible without the excellent work of:

- **Adam Machanic** for sp_WhoIsActive, the gold standard in SQL Server activity monitoring
- **Erik Darling** for DarlingData tools that provide deep system health and blocking analysis
- **Brent Ozar Unlimited** for the First Responder Kit and comprehensive SQL Server diagnostics

- **Microsoft** for vscode-mssql execution plan operator icons used in the Plan Viewer

We are grateful for their contributions to the SQL Server community and their commitment to open-source software.

---

*Last Updated: February 19, 2026*
