/* Each edition searches only its own content. Version changes start at its home. */
async function loadDocumentationVersions() {
  const select = document.getElementById("docs-version");
  if (!select || select.dataset.loaded) return;
  select.dataset.loaded = "true";
  const manifest = new URL(select.dataset.manifest, window.location.href);
  try {
    const response = await fetch(manifest);
    if (!response.ok) return; // A standalone local preview contains one edition.
    const versions = await response.json();
    const options = versions.map(({ version }, index) => {
      const label = version === "main" ? "main (development)" : `${version}${index === 0 ? " (latest release)" : ""}`;
      return new Option(label, version, false, version === select.dataset.version);
    });
    select.replaceChildren(...options);
    select.addEventListener("change", () => {
      window.location.assign(new URL(`${encodeURIComponent(select.value)}/`, manifest));
    });
  } catch {
    // Leave the current version visible when the archive manifest is unavailable.
  }
}

if (typeof document$ !== "undefined") {
  document$.subscribe(loadDocumentationVersions);
} else {
  loadDocumentationVersions();
}
