<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE xsl:stylesheet>
<xsl:stylesheet version="1.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	xmlns:sparql="http://www.w3.org/2005/sparql-results#"
	xmlns="http://www.w3.org/1999/xhtml">

	<xsl:include href="../locale/messages.xsl" />
	<xsl:include href="../locale/ergs-messages.xsl" />

	<xsl:variable name="title">
		<xsl:value-of select="$repository-create.title" />
	</xsl:variable>

	<xsl:include href="template.xsl" />

	<xsl:template match="sparql:sparql">
		<form action="create" method="post">
			<table class="dataentry">
				<tbody>
					<tr>
						<th>
							<xsl:value-of select="$repository-type.label" />
						</th>
						<td>
							<select id="type" name="type">
								<option value="ergs-repository">
									Expressive Reasoning Graph Store
								</option>
							</select>
						</td>
						<td></td>
					</tr>
					<tr>
						<th>
							<xsl:value-of select="$repository-id.label" />
						</th>
						<td>
							<input type="text" id="id" name="Repository ID" size="16"
								value="ergs-repository" />
						</td>
						<td>
							<xsl:value-of select="$ergs-table.label" />
						</td>
					</tr>
					<tr>
						<th>
							<xsl:value-of select="$repository-title.label" />
						</th>
						<td>
							<input type="text" id="title" name="Repository title"
								size="16" />
						</td>
						<td></td>
					</tr>
					<tr>
						<th>
							<xsl:value-of select="$ergs-config-file.label" />
						</th>
						<td>
							<input type="file" id="propertiesFile"
								name="propertiesFile" size="16" />

							<input type="hidden" name="config" id="config" value="" />

							<script type="text/javascript">
								document.getElementById('propertiesFile')
								.addEventListener('change', function() {

								var fr=new FileReader();
								fr.onload=function(){
								document.getElementById('config').value=escape(fr.result);
								}
								fr.readAsText(this.files[0]);
								})
							</script>

						</td>
						<td>
							<xsl:value-of
								select="$ergs-config-file-comment.label" />
						</td>
					</tr>
					<tr>
						<th>
							<xsl:value-of select="$ergs-tbox-file.label" />
						</th>
						<td>
							<input type="file" id="tboxFile" name="tboxFile" size="16" />

							<input type="hidden" name="tbox" id="tbox" value="" />

							<script type="text/javascript">
								document.getElementById('tboxFile')
								.addEventListener('change', function() {

								var fr=new FileReader();
								fr.onload=function(){
								document.getElementById('tbox').value=escape(fr.result);
								}
								fr.readAsText(this.files[0]);
								})
							</script>
						</td>
						<td>
							<xsl:value-of select="$ergs-tbox-file-comment.label" />
						</td>
					</tr>
					<tr>
						<td></td>
						<td>
							<input type="button" value="{$cancel.label}"
								style="float:right" data-href="repositories"
								onclick="document.location.href=this.getAttribute('data-href')" />
							<input id="create" type="button" value="{$create.label}"
								onclick="checkOverwrite()" />
						</td>
					</tr>
				</tbody>
			</table>
		</form>
		<script src="../../scripts/create.js" type="text/javascript">
		</script>
	</xsl:template>

</xsl:stylesheet>
