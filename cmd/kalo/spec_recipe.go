package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/kalo-build/kalo-cli/internal/specv0"
	"gopkg.in/yaml.v3"
)

// A recipe is a SPEC catalog plus recipe.yaml, published as the release asset
// recipe-<name>-<version>.tar.gz (with a .sha256 file) under the kalo-plugins
// release tag recipe-<name>-v<version>. See eng/one-shot D4 and D5.

const (
	defaultRecipeDownloadBase = "https://github.com/kalo-build/kalo-plugins/releases/download"
	defaultRecipeReleasesAPI  = "https://api.github.com/repos/kalo-build/kalo-plugins/releases?per_page=100"
	recipeFileName            = "recipe.yaml"
	maxRecipeArchiveBytes     = 256 << 20
	maxRecipeFileBytes        = 128 << 20
	maxRecipeExtractBytes     = 512 << 20
	maxRecipeEntries          = 10000
	maxReleasePages           = 20
)

var (
	recipeNamePattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	recipeVersionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$`)
	outputDirPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
)

type recipeManifest struct {
	Name        string         `yaml:"name"`
	Version     string         `yaml:"version"`
	Description string         `yaml:"description"`
	Catalog     string         `yaml:"catalog"`
	Input       recipeInput    `yaml:"input"`
	Outputs     []recipeOutput `yaml:"outputs"`
}

type recipeInput struct {
	From   string   `yaml:"from"`
	Accept []string `yaml:"accept"`
}

type recipeOutput struct {
	To  string `yaml:"to"`
	Dir string `yaml:"dir"`
}

type recipeRunOptions struct {
	Name         string
	Version      string
	LocalCatalog string // recipe folder on disk (development, CI); skips the download
	Input        string
	Out          string
	Force        bool
	ReceiptsDir  string
	KeepSpecsDir string
	Run          specRunOptions // policy and route authorisation flags
	Log          io.Writer
}

type recipeRunResult struct {
	Recipe  string            `json:"recipe"`
	Version string            `json:"version"`
	Out     string            `json:"out"`
	Outputs []recipeRunOutput `json:"outputs"`
}

type recipeRunOutput struct {
	Dir   string   `json:"dir"`
	Files []string `json:"files"`
}

func loadRecipeManifest(root string) (*recipeManifest, error) {
	data, err := os.ReadFile(filepath.Join(root, recipeFileName))
	if err != nil {
		return nil, fmt.Errorf("recipe: %w", err)
	}
	var manifest recipeManifest
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("recipe: %s: %w", recipeFileName, err)
	}
	if !recipeNamePattern.MatchString(manifest.Name) {
		return nil, fmt.Errorf("recipe: invalid name %q", manifest.Name)
	}
	if !recipeVersionPattern.MatchString(manifest.Version) {
		return nil, fmt.Errorf("recipe %s: invalid version %q", manifest.Name, manifest.Version)
	}
	if manifest.Catalog == "" {
		manifest.Catalog = "descriptors"
	}
	if clean := path.Clean(manifest.Catalog); clean != manifest.Catalog || strings.HasPrefix(clean, "../") || clean == ".." || path.IsAbs(clean) {
		return nil, fmt.Errorf("recipe %s: catalog must be a relative path inside the recipe, got %q", manifest.Name, manifest.Catalog)
	}
	if manifest.Input.From == "" {
		return nil, fmt.Errorf("recipe %s: input.from is required", manifest.Name)
	}
	if len(manifest.Outputs) == 0 {
		return nil, fmt.Errorf("recipe %s: at least one output is required", manifest.Name)
	}
	seen := map[string]bool{}
	for _, output := range manifest.Outputs {
		if output.To == "" || !outputDirPattern.MatchString(output.Dir) || seen[output.Dir] {
			return nil, fmt.Errorf("recipe %s: every output needs a target address and a unique plain dir name, got to=%q dir=%q", manifest.Name, output.To, output.Dir)
		}
		seen[output.Dir] = true
	}
	return &manifest, nil
}

func (m *recipeManifest) accepts(inputPath string) bool {
	if len(m.Input.Accept) == 0 {
		return true
	}
	base := strings.ToLower(filepath.Base(inputPath))
	for _, pattern := range m.Input.Accept {
		if ok, _ := path.Match(strings.ToLower(pattern), base); ok {
			return true
		}
	}
	return false
}

// resolveRecipe returns the folder holding recipe.yaml, fetching and caching
// the release asset when no local folder is given.
func resolveRecipe(options recipeRunOptions) (string, error) {
	if options.LocalCatalog != "" {
		return filepath.Abs(options.LocalCatalog)
	}
	if !recipeNamePattern.MatchString(options.Name) {
		return "", fmt.Errorf("invalid recipe name %q", options.Name)
	}
	client := &http.Client{Timeout: 120 * time.Second}
	version := strings.TrimPrefix(options.Version, "v")
	if version == "" && (options.Run.Policy.Offline || options.Run.Policy.DenyNetwork) {
		return "", fmt.Errorf("--offline/--deny-network need --recipe-version to use a cached recipe")
	}
	if version == "" {
		latest, err := latestRecipeVersion(client, options.Name)
		if err != nil {
			return "", err
		}
		version = latest
	}
	if !recipeVersionPattern.MatchString(version) {
		return "", fmt.Errorf("invalid recipe version %q", version)
	}
	cacheRoot := os.Getenv("KALO_CACHE_DIR")
	if cacheRoot == "" {
		userCache, err := os.UserCacheDir()
		if err != nil {
			return "", fmt.Errorf("find cache directory (set KALO_CACHE_DIR): %w", err)
		}
		cacheRoot = filepath.Join(userCache, "kalo")
	}
	target := filepath.Join(cacheRoot, "recipes", options.Name, version)
	if _, err := os.Stat(filepath.Join(target, recipeFileName)); err == nil {
		return target, nil
	}
	if options.Run.Policy.Offline || options.Run.Policy.DenyNetwork {
		return "", fmt.Errorf("recipe %s %s is not cached in %s and --offline/--deny-network forbid fetching it", options.Name, version, target)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return "", err
	}
	staging, err := os.MkdirTemp(filepath.Dir(target), ".fetch-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(staging)

	base := strings.TrimRight(envOr("KALO_RECIPE_DOWNLOAD_BASE", defaultRecipeDownloadBase), "/")
	asset := fmt.Sprintf("recipe-%s-%s.tar.gz", options.Name, version)
	assetURL := fmt.Sprintf("%s/recipe-%s-v%s/%s", base, options.Name, version, asset)
	if options.Log != nil {
		fmt.Fprintf(options.Log, "fetching recipe %s %s\n", options.Name, version)
	}
	sumText, err := httpGet(client, assetURL+".sha256", 4096)
	if err != nil {
		return "", fmt.Errorf("recipe %s %s: %w", options.Name, version, err)
	}
	fields := strings.Fields(string(sumText))
	if len(fields) == 0 || len(fields[0]) != 64 {
		return "", fmt.Errorf("recipe %s %s: malformed %s.sha256", options.Name, version, asset)
	}
	archive, err := httpGet(client, assetURL, maxRecipeArchiveBytes)
	if err != nil {
		return "", fmt.Errorf("recipe %s %s: %w", options.Name, version, err)
	}
	sum := sha256.Sum256(archive)
	if got := hex.EncodeToString(sum[:]); !strings.EqualFold(got, fields[0]) {
		return "", fmt.Errorf("recipe %s %s: sha256 is %s, release says %s", options.Name, version, got, fields[0])
	}
	extracted := filepath.Join(staging, "recipe")
	if err := extractTarGz(archive, extracted); err != nil {
		return "", fmt.Errorf("recipe %s %s: %w", options.Name, version, err)
	}
	manifest, err := loadRecipeManifest(extracted)
	if err != nil {
		return "", err
	}
	if manifest.Name != options.Name || manifest.Version != version {
		return "", fmt.Errorf("recipe asset %s contains %s %s", asset, manifest.Name, manifest.Version)
	}
	if err := os.Rename(extracted, target); err != nil {
		if _, statErr := os.Stat(filepath.Join(target, recipeFileName)); statErr == nil {
			return target, nil // a concurrent run cached it first
		}
		return "", err
	}
	return target, nil
}

func latestRecipeVersion(client *http.Client, name string) (string, error) {
	type release struct {
		TagName string `json:"tag_name"`
		Draft   bool   `json:"draft"`
	}
	var releases []release
	// Follow the API's next-page links: recipes and plugins share one releases list.
	url := envOr("KALO_RECIPE_RELEASES_API", defaultRecipeReleasesAPI)
	for page := 0; url != "" && page < maxReleasePages; page++ {
		data, next, err := httpGetPage(client, url, 8<<20)
		if err != nil {
			var status *httpStatusError
			if errors.As(err, &status) && (status.Code == http.StatusForbidden || status.Code == http.StatusTooManyRequests) {
				return "", fmt.Errorf("find latest version of recipe %s: GitHub API refused the request (%s), usually the rate limit; pass --recipe-version <version> to skip the lookup", name, status.Status)
			}
			return "", fmt.Errorf("find latest version of recipe %s (or pass --recipe-version): %w", name, err)
		}
		var pageReleases []release
		if err := json.Unmarshal(data, &pageReleases); err != nil {
			return "", fmt.Errorf("find latest version of recipe %s: %w", name, err)
		}
		releases = append(releases, pageReleases...)
		if next != "" && !sameHost(next, url) {
			return "", fmt.Errorf("find latest version of recipe %s: next-page link %s leaves the API host; pass --recipe-version <version>", name, next)
		}
		url = next
	}
	prefix := "recipe-" + name + "-v"
	var best *semver.Version
	for _, release := range releases {
		if release.Draft || !strings.HasPrefix(release.TagName, prefix) {
			continue
		}
		raw := strings.TrimPrefix(release.TagName, prefix)
		if !recipeVersionPattern.MatchString(raw) {
			continue
		}
		version, err := semver.StrictNewVersion(raw)
		if err == nil && (best == nil || version.GreaterThan(best)) {
			best = version
		}
	}
	if best == nil {
		return "", fmt.Errorf("no published recipe named %q (releases tagged %s<version> on kalo-build/kalo-plugins)", name, prefix)
	}
	return best.String(), nil
}

type httpStatusError struct {
	URL    string
	Code   int
	Status string
}

func (e *httpStatusError) Error() string { return fmt.Sprintf("GET %s: %s", e.URL, e.Status) }

var nextLinkPattern = regexp.MustCompile(`<([^>]+)>;\s*rel="next"`)

func httpGet(client *http.Client, url string, limit int64) ([]byte, error) {
	data, _, err := httpGetPage(client, url, limit)
	return data, err
}

// sameHost reports whether two absolute URLs share scheme and host.
func sameHost(a, b string) bool {
	ua, errA := neturl.Parse(a)
	ub, errB := neturl.Parse(b)
	return errA == nil && errB == nil && ua.Host != "" && ua.Scheme == ub.Scheme && ua.Host == ub.Host
}

// httpGetPage also returns the rel="next" URL of a Link header, if any.
func httpGetPage(client *http.Client, url string, limit int64) ([]byte, string, error) {
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, "", err
	}
	// Plain GET: no identifiers, nothing about the user is sent (no telemetry).
	request.Header.Set("User-Agent", "kalo/"+version)
	response, err := client.Do(request)
	if err != nil {
		return nil, "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, "", &httpStatusError{URL: url, Code: response.StatusCode, Status: response.Status}
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, "", err
	}
	if int64(len(data)) > limit {
		return nil, "", fmt.Errorf("GET %s: response larger than %d bytes", url, limit)
	}
	next := ""
	if match := nextLinkPattern.FindStringSubmatch(response.Header.Get("Link")); match != nil {
		next = match[1]
	}
	return data, next, nil
}

// extractTarGz writes regular files and directories only; every name must be
// a clean relative path. Links and devices are refused.
func extractTarGz(archive []byte, root string) error {
	gz, err := gzip.NewReader(strings.NewReader(string(archive)))
	if err != nil {
		return err
	}
	defer gz.Close()
	reader := tar.NewReader(gz)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	var total int64
	for entries := 1; ; entries++ {
		header, err := reader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if entries > maxRecipeEntries {
			return fmt.Errorf("archive has more than %d entries", maxRecipeEntries)
		}
		name := strings.TrimPrefix(header.Name, "./")
		if name == "" || name == "." {
			continue
		}
		clean := path.Clean(name)
		if strings.Contains(name, "\\") || path.IsAbs(name) || clean == ".." || strings.HasPrefix(clean, "../") {
			return fmt.Errorf("archive entry %q escapes the recipe folder", header.Name)
		}
		target := filepath.Join(root, filepath.FromSlash(clean))
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if header.Size > maxRecipeFileBytes {
				return fmt.Errorf("archive entry %q is larger than %d bytes", header.Name, maxRecipeFileBytes)
			}
			if total += header.Size; total > maxRecipeExtractBytes {
				return fmt.Errorf("archive expands to more than %d bytes", maxRecipeExtractBytes)
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(file, io.LimitReader(reader, maxRecipeFileBytes))
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		default:
			return fmt.Errorf("archive entry %q is not a regular file or directory", header.Name)
		}
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// requireEmptyOrMissing refuses a directory that has entries, unless force.
func requireEmptyOrMissing(dir, flag string, force bool) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%s %s: %w", flag, dir, err)
	}
	if len(entries) > 0 && !force {
		return fmt.Errorf("%s %s is not empty (use --force to replace the recipe's output folders in it)", flag, dir)
	}
	return nil
}

// runRecipe runs every output route of a recipe on one input file and
// materialises the final values into --out/<dir>. All temporary files (work
// dirs, staged values, receipts) are removed on success and on failure; only
// --receipts and --keep-specs keep anything else.
func runRecipe(options recipeRunOptions) (*recipeRunResult, error) {
	recipeRoot, err := resolveRecipe(options)
	if err != nil {
		return nil, err
	}
	manifest, err := loadRecipeManifest(recipeRoot)
	if err != nil {
		return nil, err
	}
	if options.LocalCatalog == "" && options.Name != manifest.Name {
		return nil, fmt.Errorf("recipe folder holds %s, not %s", manifest.Name, options.Name)
	}
	inputPath, err := filepath.Abs(options.Input)
	if err != nil {
		return nil, err
	}
	if info, err := os.Stat(inputPath); err != nil || info.IsDir() {
		return nil, fmt.Errorf("--input must be one readable file: %s", options.Input)
	}
	if !manifest.accepts(inputPath) {
		return nil, fmt.Errorf("recipe %s accepts %s, got %s", manifest.Name, strings.Join(manifest.Input.Accept, ", "), filepath.Base(inputPath))
	}
	outRoot, err := filepath.Abs(options.Out)
	if err != nil {
		return nil, err
	}
	if err := requireEmptyOrMissing(outRoot, "--out", options.Force); err != nil {
		return nil, err
	}
	for flag, dir := range map[string]string{"--receipts": options.ReceiptsDir, "--keep-specs": options.KeepSpecsDir} {
		if dir != "" {
			if err := requireEmptyOrMissing(dir, flag, false); err != nil {
				return nil, err
			}
		}
	}
	if err := checkDestinations(recipeRoot, outRoot, manifest, options); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(outRoot), 0o755); err != nil {
		return nil, err
	}
	// Work next to --out so the final move is a rename on the same file system.
	workRoot, err := os.MkdirTemp(filepath.Dir(outRoot), ".kalo-run-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(workRoot)

	catalogDir := filepath.Join(recipeRoot, filepath.FromSlash(manifest.Catalog))
	result := &recipeRunResult{Recipe: manifest.Name, Version: manifest.Version, Out: options.Out}
	runRoots := map[string]string{}
	routes := map[string][2]string{}
	staged := filepath.Join(workRoot, "out")
	catalog, err := specv0.Load(catalogDir)
	if err != nil {
		return nil, err
	}
	for _, output := range manifest.Outputs {
		run := options.Run
		run.CatalogPath, run.From, run.To, run.Input = catalogDir, manifest.Input.From, output.To, inputPath
		run.Output = filepath.Join(workRoot, "runs", output.Dir)
		if options.Log != nil {
			fmt.Fprintf(options.Log, "running %s -> %s\n", manifest.Name, output.Dir)
		}
		execution, err := runSPEC(run)
		if err != nil {
			return nil, fmt.Errorf("recipe %s, output %s: %w", manifest.Name, output.Dir, err)
		}
		runRoots[output.Dir] = run.Output
		routes[output.Dir] = [2]string{run.From, run.To}
		finalValue := filepath.Join(run.Output, filepath.FromSlash(execution.Output))
		finalBinding := execution.Route.Steps[len(execution.Route.Steps)-1].OutputBinding
		dest := filepath.Join(staged, output.Dir)
		var files []string
		if binding := catalog.Bindings[finalBinding]; binding != nil && specv0.IsFileSetMediaType(binding.MediaType) {
			data, err := os.ReadFile(finalValue)
			if err != nil {
				return nil, err
			}
			if files, err = specv0.UnpackFileSet(data, dest); err != nil {
				return nil, err
			}
		} else {
			// Staged values are named "<index>-<name>"; keep <name>.
			name := filepath.Base(finalValue)
			if _, rest, found := strings.Cut(name, "-"); found && rest != "" {
				name = rest
			}
			if err := copyFile(finalValue, filepath.Join(dest, name)); err != nil {
				return nil, err
			}
			files = []string{name}
		}
		sort.Strings(files)
		result.Outputs = append(result.Outputs, recipeRunOutput{Dir: output.Dir, Files: files})
	}

	if options.ReceiptsDir != "" {
		for dir, runRoot := range runRoots {
			if err := copyTree(filepath.Join(runRoot, "receipts"), filepath.Join(options.ReceiptsDir, dir)); err != nil {
				return nil, err
			}
			if err := copyFile(filepath.Join(runRoot, "execution.json"), filepath.Join(options.ReceiptsDir, dir, "execution.json")); err != nil {
				return nil, err
			}
		}
	}
	if options.KeepSpecsDir != "" {
		if err := keepSpecs(options.KeepSpecsDir, recipeRoot, manifest, runRoots, routes, filepath.Base(inputPath)); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(outRoot, 0o755); err != nil {
		return nil, err
	}
	for _, output := range manifest.Outputs {
		dest := filepath.Join(outRoot, output.Dir)
		if err := os.RemoveAll(dest); err != nil {
			return nil, err
		}
		if err := os.Rename(filepath.Join(staged, output.Dir), dest); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// keepSpecs moves a one-shot run into the persistent flow: the catalog, every
// intermediate value and receipt, and a README with the lines that rerun it.
func keepSpecs(dir, recipeRoot string, manifest *recipeManifest, runRoots map[string]string, routes map[string][2]string, inputName string) error {
	if err := copyTree(recipeRoot, filepath.Join(dir, "recipe")); err != nil {
		return err
	}
	var readme strings.Builder
	fmt.Fprintf(&readme, "# %s %s: kept specs\n\nWritten by `kalo spec run %s --keep-specs`. `recipe/` is the catalog that ran; `runs/<output>/` holds every intermediate value (`artifacts/`), the Receipts and `execution.json`.\n\nRerun one output from the kept catalog (run from this folder):\n\n```sh\n", manifest.Name, manifest.Version, manifest.Name)
	for _, output := range manifest.Outputs {
		if err := copyTree(runRoots[output.Dir], filepath.Join(dir, "runs", output.Dir)); err != nil {
			return err
		}
		route := routes[output.Dir]
		fmt.Fprintf(&readme, "kalo spec run --catalog %s --from %s --to %s --input %s --output %s\n",
			shellQuote("recipe/"+manifest.Catalog), shellQuote(route[0]), shellQuote(route[1]),
			shellQuote("runs/"+output.Dir+"/artifacts/00-"+inputName), shellQuote("rerun-"+output.Dir))
	}
	readme.WriteString("```\n\nOr run the whole recipe again: `kalo spec run --recipe-dir recipe --input <file> --out <dir>`.\n")
	return os.WriteFile(filepath.Join(dir, "README.md"), []byte(readme.String()), 0o644)
}

// checkDestinations runs before anything is written: the recipe's output
// folders, --receipts and --keep-specs must not overlap, and evidence must not
// land inside the recipe folder it copies.
func checkDestinations(recipeRoot, outRoot string, manifest *recipeManifest, options recipeRunOptions) error {
	recipeRoot, err := filepath.Abs(recipeRoot)
	if err != nil {
		return err
	}
	type destination struct{ flag, path string }
	var destinations []destination
	for _, output := range manifest.Outputs {
		path := filepath.Join(outRoot, output.Dir)
		if pathWithin(path, recipeRoot) || pathWithin(recipeRoot, path) {
			return fmt.Errorf("--out %s overlaps the recipe folder %s", path, recipeRoot)
		}
		destinations = append(destinations, destination{"--out " + output.Dir, path})
	}
	for flag, dir := range map[string]string{"--receipts": options.ReceiptsDir, "--keep-specs": options.KeepSpecsDir} {
		if dir == "" {
			continue
		}
		abs, err := filepath.Abs(dir)
		if err != nil {
			return err
		}
		if pathWithin(abs, recipeRoot) || pathWithin(recipeRoot, abs) {
			return fmt.Errorf("%s %s overlaps the recipe folder %s", flag, dir, recipeRoot)
		}
		destinations = append(destinations, destination{flag, abs})
	}
	for i := range destinations {
		for j := i + 1; j < len(destinations); j++ {
			a, b := destinations[i], destinations[j]
			if pathWithin(a.path, b.path) || pathWithin(b.path, a.path) {
				return fmt.Errorf("%s (%s) and %s (%s) overlap; give them separate folders", a.flag, a.path, b.flag, b.path)
			}
		}
	}
	return nil
}

// pathWithin reports whether child is parent or below it (both absolute).
func pathWithin(child, parent string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

// shellQuote quotes s for POSIX shells unless it is plainly safe.
func shellQuote(s string) string {
	if s != "" && strings.IndexFunc(s, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("-_./:@+=", r))
	}) < 0 {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func copyTree(source, destination string) error {
	return filepath.WalkDir(source, func(current string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(source, current)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("copy %s: not a regular file", current)
		}
		return copyFile(current, target)
	})
}
